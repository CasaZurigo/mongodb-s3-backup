import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { MongoClient } from "mongodb";
import { EJSON } from "bson";
import { createWriteStream, createReadStream, unlinkSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { createGunzip } from "zlib";
import { Readable } from "stream";
import dotenv from "dotenv";

dotenv.config();

const {
  S3_ENDPOINT,
  S3_REGION,
  S3_ACCESS_KEY_ID,
  S3_SECRET_ACCESS_KEY,
  S3_BUCKET,
  S3_KEY_PATH,
  MONGODB_URI,
} = process.env;

if (
  !S3_REGION ||
  !S3_ACCESS_KEY_ID ||
  !S3_SECRET_ACCESS_KEY ||
  !S3_BUCKET ||
  !MONGODB_URI
) {
  console.error("Missing required environment variables");
  process.exit(1);
}

const s3Client = new S3Client({
  endpoint: S3_ENDPOINT,
  region: S3_REGION,
  credentials: {
    accessKeyId: S3_ACCESS_KEY_ID,
    secretAccessKey: S3_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

interface BackupData {
  databases: {
    [dbName: string]: {
      collections: {
        [collName: string]: {
          documents: any[];
          indexes: any[];
        };
      };
    };
  };
  timestamp: string;
}

async function listBackups(): Promise<{ key: string; lastModified: Date }[]> {
  const prefix = S3_KEY_PATH
    ? `${S3_KEY_PATH}/mongodb-backup-`
    : "mongodb-backup-";

  const listCommand = new ListObjectsV2Command({
    Bucket: S3_BUCKET,
    Prefix: prefix,
  });

  const response = await s3Client.send(listCommand);

  if (!response.Contents || response.Contents.length === 0) {
    return [];
  }

  return response.Contents.filter((obj) => obj.Key && obj.LastModified)
    .map((obj) => ({
      key: obj.Key!,
      lastModified: obj.LastModified!,
    }))
    .sort((a, b) => b.lastModified.getTime() - a.lastModified.getTime());
}

async function downloadBackup(s3Key: string): Promise<string> {
  const fileName = s3Key.split("/").pop() || "backup.gz";
  const tempFilePath = join(tmpdir(), fileName);

  console.log(`Downloading backup from S3: ${s3Key}`);

  const getCommand = new GetObjectCommand({
    Bucket: S3_BUCKET,
    Key: s3Key,
  });

  const response = await s3Client.send(getCommand);

  if (!response.Body) {
    throw new Error("Empty response body from S3");
  }

  const writeStream = createWriteStream(tempFilePath);

  await new Promise<void>((resolve, reject) => {
    const body = response.Body as Readable;
    body.pipe(writeStream);
    writeStream.on("finish", resolve);
    writeStream.on("error", reject);
  });

  console.log(`Backup downloaded to: ${tempFilePath}`);
  return tempFilePath;
}

async function decompressBackup(filePath: string): Promise<BackupData> {
  console.log("Decompressing backup...");

  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    const gunzip = createGunzip();
    const readStream = createReadStream(filePath);

    readStream
      .pipe(gunzip)
      .on("data", (chunk: Buffer) => chunks.push(chunk))
      .on("end", () => {
        try {
          const data = Buffer.concat(chunks).toString("utf-8");
          // Use EJSON.parse to properly deserialize all BSON types
          // (ObjectId, Date, Binary, Decimal128, Int64, UUID, etc.)
          const backup = EJSON.parse(data) as BackupData;
          resolve(backup);
        } catch (error) {
          reject(new Error(`Failed to parse backup JSON: ${error}`));
        }
      })
      .on("error", reject);
  });
}

async function restoreToMongoDB(
  backup: BackupData,
  dropExisting: boolean = false
): Promise<void> {
  const client = new MongoClient(MONGODB_URI!);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    // Check if a specific database is specified in the URI
    const url = new URL(MONGODB_URI!);
    const specificDatabase = url.pathname.slice(1);

    for (const [dbName, dbData] of Object.entries(backup.databases)) {
      // If URI specifies a database, only restore to that database
      if (
        specificDatabase &&
        specificDatabase !== "" &&
        dbName !== specificDatabase
      ) {
        console.log(
          `Skipping database ${dbName} (URI specifies ${specificDatabase})`
        );
        continue;
      }

      console.log(`Restoring database: ${dbName}`);
      const db = client.db(dbName);

      for (const [collName, collData] of Object.entries(dbData.collections)) {
        console.log(`  Restoring collection: ${collName}`);
        const collection = db.collection(collName);

        if (dropExisting) {
          try {
            await collection.drop();
            console.log(`    Dropped existing collection: ${collName}`);
          } catch (error: any) {
            // Collection might not exist, ignore error
            if (error.codeName !== "NamespaceNotFound") {
              console.warn(`    Warning dropping collection: ${error.message}`);
            }
          }
        }

        if (collData.documents && collData.documents.length > 0) {
          try {
            // EJSON.parse already converted all BSON types (ObjectId, Date, etc.)
            await collection.insertMany(collData.documents, { ordered: false });
            console.log(`    Inserted ${collData.documents.length} documents`);
          } catch (error: any) {
            if (error.code === 11000) {
              // Duplicate key error - some documents already exist
              const insertedCount = error.result?.insertedCount || 0;
              console.log(
                `    Inserted ${insertedCount} documents (${
                  collData.documents.length - insertedCount
                } duplicates skipped)`
              );
            } else {
              throw error;
            }
          }
        }

        // Restore indexes (skip _id index as it's created automatically)
        if (collData.indexes && collData.indexes.length > 0) {
          for (const index of collData.indexes) {
            if (index.name === "_id_") continue; // Skip default _id index

            try {
              const indexSpec = { ...index };
              delete indexSpec.v;
              delete indexSpec.ns;

              const { key, ...options } = indexSpec;
              await collection.createIndex(key, options);
              console.log(`    Created index: ${index.name}`);
            } catch (error: any) {
              if (error.code === 85 || error.code === 86) {
                // Index already exists with same name or same spec
                console.log(`    Index already exists: ${index.name}`);
              } else {
                console.warn(
                  `    Warning creating index ${index.name}: ${error.message}`
                );
              }
            }
          }
        }
      }
    }

    console.log("Restore completed successfully");
  } finally {
    await client.close();
    console.log("MongoDB connection closed");
  }
}

async function restore(
  backupFileName?: string,
  dropExisting: boolean = false
): Promise<void> {
  let s3Key: string;

  if (backupFileName) {
    // Use specific backup file
    s3Key = S3_KEY_PATH ? `${S3_KEY_PATH}/${backupFileName}` : backupFileName;
    console.log(`Using specified backup: ${s3Key}`);
  } else {
    // Find the latest backup
    console.log("Finding latest backup...");
    const backups = await listBackups();
    const backup = backups[0];

    if (!backup) {
      console.error("No backups found in S3");
      process.exit(1);
    }

    s3Key = backup.key;
    console.log(
      `Latest backup: ${s3Key} (${backup.lastModified.toISOString()})`
    );
  }

  let tempFilePath: string | null = null;

  try {
    // Download backup from S3
    tempFilePath = await downloadBackup(s3Key);

    // Decompress and parse backup
    const backup = await decompressBackup(tempFilePath);
    console.log(`Backup timestamp: ${backup.timestamp}`);
    console.log(
      `Databases in backup: ${Object.keys(backup.databases).join(", ")}`
    );

    // Restore to MongoDB
    await restoreToMongoDB(backup, dropExisting);
  } finally {
    // Cleanup temp file
    if (tempFilePath) {
      try {
        unlinkSync(tempFilePath);
        console.log("Temporary file cleaned up");
      } catch (error) {
        console.error("Failed to cleanup temporary file:", error);
      }
    }
  }
}

function printUsage(): void {
  console.log(`
  MongoDB S3 Restore Tool
  
  Usage:
    bun run restore.ts [options]
  
  Options:
    --file, -f <filename>    Restore from a specific backup file (e.g., mongodb-backup-20241227.gz)
    --latest, -l             Restore from the latest backup (default)
    --drop, -d               Drop existing collections before restoring
    --list                   List all available backups
    --help, -h               Show this help message
  
  Examples:
    bun run restore.ts                                    # Restore latest backup
    bun run restore.ts --latest                           # Restore latest backup
    bun run restore.ts --file mongodb-backup-20241227.gz  # Restore specific backup
    bun run restore.ts --drop                             # Drop collections before restore
    bun run restore.ts --list                             # List available backups
  
  Environment Variables (required):
    S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET, MONGODB_URI
  
  Optional Environment Variables:
    S3_ENDPOINT, S3_KEY_PATH
  `);
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.includes("--help") || args.includes("-h")) {
    printUsage();
    process.exit(0);
  }

  if (args.includes("--list")) {
    console.log("Available backups:\n");
    const backups = await listBackups();

    if (backups.length === 0) {
      console.log("No backups found");
    } else {
      for (const backup of backups) {
        const fileName = backup.key.split("/").pop();
        console.log(`  ${fileName}  (${backup.lastModified.toISOString()})`);
      }
    }
    process.exit(0);
  }

  let backupFileName: string | undefined;
  let dropExisting = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === "--file" || arg === "-f") {
      backupFileName = args[++i];
      if (!backupFileName) {
        console.error("Error: --file requires a filename argument");
        process.exit(1);
      }
    } else if (arg === "--drop" || arg === "-d") {
      dropExisting = true;
    } else if (arg === "--latest" || arg === "-l") {
      // Default behavior, do nothing
    } else if (arg && !arg.startsWith("-")) {
      // Treat as filename if not a flag
      backupFileName = arg;
    }
  }

  if (dropExisting) {
    console.log(
      "WARNING: --drop flag is set. Existing collections will be dropped!"
    );
    console.log("Starting restore in 3 seconds... (Ctrl+C to cancel)\n");
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }

  try {
    await restore(backupFileName, dropExisting);
  } catch (error) {
    console.error("Restore failed:", error);
    process.exit(1);
  }
}

main();
