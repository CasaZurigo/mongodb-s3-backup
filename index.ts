import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { CronJob } from "cron";
import { MongoClient } from "mongodb";
import { createReadStream, createWriteStream, unlinkSync, statSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { createGzip } from "zlib";
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
  CRON_SCHEDULE,
  MONGODB_URI,
  RETENTION_DAYS,
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

async function deleteOldBackups(): Promise<void> {
  if (!RETENTION_DAYS) {
    console.log("No retention period set, skipping cleanup");
    return;
  }

  const retentionDays = parseInt(RETENTION_DAYS, 10);
  if (isNaN(retentionDays) || retentionDays <= 0) {
    console.log("Invalid retention period, skipping cleanup");
    return;
  }

  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

  console.log(
    `Cleaning up backups older than ${retentionDays} days (before ${cutoffDate.toISOString()})`
  );

  try {
    const listCommand = new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: S3_KEY_PATH
        ? `${S3_KEY_PATH}/mongodb-backup-`
        : "mongodb-backup-",
    });

    const response = await s3Client.send(listCommand);

    if (!response.Contents) {
      console.log("No backups found");
      return;
    }

    const objectsToDelete = response.Contents.filter((obj) => {
      if (!obj.LastModified) return false;
      return obj.LastModified < cutoffDate;
    });

    if (objectsToDelete.length === 0) {
      console.log("No old backups to delete");
      return;
    }

    console.log(`Found ${objectsToDelete.length} old backup(s) to delete`);

    for (const obj of objectsToDelete) {
      if (obj.Key) {
        const deleteCommand = new DeleteObjectCommand({
          Bucket: S3_BUCKET,
          Key: obj.Key,
        });

        await s3Client.send(deleteCommand);
        console.log(`Deleted old backup: ${obj.Key}`);
      }
    }

    console.log(
      `Cleanup completed: ${objectsToDelete.length} old backup(s) deleted`
    );
  } catch (error) {
    console.error("Failed to cleanup old backups:", error);
  }
}

async function createMongoDBBackup(): Promise<void> {
  const timestamp = new Date().toISOString().split("T")[0]!.replace(/-/g, "");
  const backupFileName = `mongodb-backup-${timestamp}.gz`;
  const tempFilePath = join(tmpdir(), backupFileName);

  console.log(`Starting MongoDB backup: ${backupFileName}`);

  const client = new MongoClient(MONGODB_URI!);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const backup: any = {
      databases: {},
      timestamp: new Date().toISOString(),
    };

    // Check if a specific database is specified in the URI
    const url = new URL(MONGODB_URI!);
    const specificDatabase = url.pathname.slice(1); // Remove leading slash

    let databasesToBackup: string[] = [];

    if (specificDatabase && specificDatabase !== "") {
      // If specific database is in URI, only backup that one
      console.log(`URI specifies database: ${specificDatabase}`);
      databasesToBackup = [specificDatabase];
    } else {
      // Otherwise, backup all user databases
      const admin = client.db().admin();
      const databases = await admin.listDatabases();
      databasesToBackup = databases.databases
        .filter((dbInfo) => !["admin", "local", "config"].includes(dbInfo.name))
        .map((dbInfo) => dbInfo.name);
    }

    for (const dbName of databasesToBackup) {
      console.log(`Backing up database: ${dbName}`);
      const db = client.db(dbName);
      const collections = await db.listCollections().toArray();

      backup.databases[dbName] = {
        collections: {},
      };

      for (const collInfo of collections) {
        console.log(`  Backing up collection: ${collInfo.name}`);
        const collection = db.collection(collInfo.name);
        const documents = await collection.find({}).toArray();
        backup.databases[dbName].collections[collInfo.name] = {
          documents,
          indexes: await collection.listIndexes().toArray(),
        };
      }
    }

    const backupJson = JSON.stringify(backup, null, 2);
    const writeStream = createWriteStream(tempFilePath);
    const gzipStream = createGzip();

    const readable = Readable.from([backupJson]);
    readable.pipe(gzipStream).pipe(writeStream);

    await new Promise<void>((resolve, reject) => {
      writeStream.on("finish", resolve);
      writeStream.on("error", reject);
    });

    console.log("MongoDB backup completed, uploading to S3...");

    const fileStream = createReadStream(tempFilePath);
    const s3Key = S3_KEY_PATH
      ? `${S3_KEY_PATH}/${backupFileName}`
      : backupFileName;

    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: S3_BUCKET,
        Key: s3Key,
        Body: fileStream,
      },
    });

    await upload.done();

    console.log(`Backup uploaded successfully to S3: ${s3Key}`);

    unlinkSync(tempFilePath);
    console.log("Temporary file cleaned up");

    await deleteOldBackups();
  } catch (error) {
    console.error("Backup failed:", error);

    try {
      unlinkSync(tempFilePath);
    } catch (cleanupError) {
      console.error("Failed to cleanup temporary file:", cleanupError);
    }

    throw error;
  } finally {
    await client.close();
    console.log("MongoDB connection closed");
  }
}

if (CRON_SCHEDULE) {
  console.log(`Setting up cron job with schedule: ${CRON_SCHEDULE}`);
  new CronJob(CRON_SCHEDULE, createMongoDBBackup, null, true);
  console.log("Cron job started");
} else {
  console.log("No cron schedule specified, running backup once...");
  createMongoDBBackup().catch(console.error);
}
