import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { CronJob } from "cron";
import { spawn } from "child_process";
import { createReadStream, unlinkSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
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
} = process.env;

if (
  !S3_ENDPOINT ||
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

async function createMongoDBBackup(): Promise<void> {
  const timestamp = new Date().toISOString().split("T")[0]!.replace(/-/g, "");
  const backupFileName = `mongodb-backup-${timestamp}.gz`;
  const tempFilePath = join(tmpdir(), backupFileName);

  console.log(`Starting MongoDB backup: ${backupFileName}`);

  try {
    await new Promise<void>((resolve, reject) => {
      const mongodump = spawn("mongodump", [
        "--uri",
        MONGODB_URI!,
        "--archive",
        "--gzip",
      ]);

      const gzipStream = require("fs").createWriteStream(tempFilePath);

      mongodump.stdout.pipe(gzipStream);

      mongodump.on("error", reject);
      mongodump.on("close", (code) => {
        if (code === 0) {
          resolve();
        } else {
          reject(new Error(`mongodump exited with code ${code}`));
        }
      });
    });

    console.log("MongoDB dump completed, uploading to S3...");

    const fileStream = createReadStream(tempFilePath);
    const s3Key = S3_KEY_PATH
      ? `${S3_KEY_PATH}/${backupFileName}`
      : backupFileName;

    const uploadCommand = new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
      Body: fileStream,
    });

    await s3Client.send(uploadCommand);

    console.log(`Backup uploaded successfully to S3: ${s3Key}`);

    unlinkSync(tempFilePath);
    console.log("Temporary file cleaned up");
  } catch (error) {
    console.error("Backup failed:", error);

    try {
      unlinkSync(tempFilePath);
    } catch (cleanupError) {
      console.error("Failed to cleanup temporary file:", cleanupError);
    }

    throw error;
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
