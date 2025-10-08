import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectCommand,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";
import dotenv from "dotenv";
import { sequelize } from "../config/database";
import { ForumPost } from "../model/ForumPost";

dotenv.config();

interface S3FileInfo {
  key: string;
  fullUrl: string;
  fileName: string;
  isThumb: boolean;
  size: number;
  lastModified: Date;
}

class S3FileList {
  private s3Client: S3Client;
  private bucketName: string;
  private region: string;

  constructor() {
    this.bucketName = process.env.S3_BUCKET_NAME || "";
    this.region = process.env.S3_REGION || "us-east-2";

    // Ensure endpoint has protocol
    let endpoint =
      process.env.S3_ENDPOINT || `https://s3.${this.region}.wasabisys.com`;
    if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
      endpoint = `https://${endpoint}`;
    }

    this.s3Client = new S3Client({
      region: this.region,
      endpoint: endpoint,
      credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY_ID || "",
        secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || "",
      },
    });
  }

  /**
   * Check if a file is an image based on extension
   */
  private isImageFile(fileName: string): boolean {
    const imageExtensions = [
      ".jpg",
      ".jpeg",
      ".png",
      ".gif",
      ".bmp",
      ".webp",
      ".svg",
    ];
    const lowerFileName = fileName.toLowerCase();
    return imageExtensions.some((ext) => lowerFileName.endsWith(ext));
  }

  /**
   * Get all IMAGE files in a specific folder (threadId/postId) from S3 bucket
   * @param threadId - The thread ID
   * @param postId - The post ID
   * @returns Array of image file information only
   */
  async getFilesByThreadAndPost(
    threadId: number,
    postId: number
  ): Promise<S3FileInfo[]> {
    try {
      const prefix = `forum-media/${threadId}/${postId}/`;

      console.log(`Listing files in S3 bucket: ${this.bucketName}`);
      console.log(`Prefix: ${prefix}`);

      const command = new ListObjectsV2Command({
        Bucket: this.bucketName,
        Prefix: prefix,
      });

      const response = await this.s3Client.send(command);

      if (!response.Contents || response.Contents.length === 0) {
        console.log(
          `No files found for threadId: ${threadId}, postId: ${postId}`
        );
        return [];
      }

      // Filter and map only image files
      const allFiles = response.Contents.map((item) => {
        const key = item.Key || "";
        const fileName = key.split("/").pop() || "";
        const isThumb = fileName.includes("_thumb");
        const fullUrl = `https://${this.bucketName}.s3.${this.region}.wasabisys.com/${key}`;

        return {
          key,
          fullUrl,
          fileName,
          isThumb,
          size: item.Size || 0,
          lastModified: item.LastModified || new Date(),
        };
      });

      // Filter to get only image files
      const imageFiles = allFiles.filter((file) =>
        this.isImageFile(file.fileName)
      );

      console.log(
        `Found ${imageFiles.length} image file(s) (out of ${allFiles.length} total) for threadId: ${threadId}, postId: ${postId}`
      );

      return imageFiles;
    } catch (error) {
      console.error(`Error listing files from S3:`, error);
      throw error;
    }
  }

  /**
   * Delete a single file from S3
   */
  async deleteFile(key: string): Promise<boolean> {
    try {
      const command = new DeleteObjectCommand({
        Bucket: this.bucketName,
        Key: key,
      });

      await this.s3Client.send(command);
      console.log(`✓ Deleted: ${key}`);
      return true;
    } catch (error) {
      console.error(`✗ Failed to delete: ${key}`, error);
      return false;
    }
  }

  /**
   * Delete multiple files from S3 (batch delete - up to 1000 files at once)
   */
  async deleteFiles(
    keys: string[]
  ): Promise<{ successful: number; failed: number }> {
    if (keys.length === 0) {
      return { successful: 0, failed: 0 };
    }

    let successful = 0;
    let failed = 0;

    // S3 DeleteObjects can handle up to 1000 objects at once
    const BATCH_SIZE = 1000;

    for (let i = 0; i < keys.length; i += BATCH_SIZE) {
      const batch = keys.slice(i, i + BATCH_SIZE);

      try {
        const command = new DeleteObjectsCommand({
          Bucket: this.bucketName,
          Delete: {
            Objects: batch.map((key) => ({ Key: key })),
            Quiet: false,
          },
        });

        const response = await this.s3Client.send(command);

        if (response.Deleted) {
          successful += response.Deleted.length;
          console.log(
            `✓ Deleted ${response.Deleted.length} files (batch ${
              Math.floor(i / BATCH_SIZE) + 1
            })`
          );
        }

        if (response.Errors && response.Errors.length > 0) {
          failed += response.Errors.length;
          console.error(`✗ Failed to delete ${response.Errors.length} files`);
          response.Errors.forEach((error) => {
            console.error(`  - ${error.Key}: ${error.Message}`);
          });
        }
      } catch (error) {
        console.error(
          `✗ Batch delete failed for ${batch.length} files:`,
          error
        );
        failed += batch.length;
      }
    }

    return { successful, failed };
  }

  /**
   * Delete all files for a specific post
   */
  async deletePostFiles(
    threadId: number,
    postId: number
  ): Promise<{ successful: number; failed: number }> {
    console.log(
      `\n=== Deleting files for Thread: ${threadId}, Post: ${postId} ===`
    );

    const files = await this.getFilesByThreadAndPost(threadId, postId);
    const keys = files.map((f) => f.key);

    if (keys.length === 0) {
      console.log("No files to delete.");
      return { successful: 0, failed: 0 };
    }

    console.log(`Found ${keys.length} file(s) to delete...`);
    return await this.deleteFiles(keys);
  }

  /**
   * Delete ALL media files from S3 by getting post IDs from forum_posts table
   * Processes posts in batches for better performance
   */
  async deleteAllMediaFromDatabase(): Promise<void> {
    const BATCH_SIZE = 300;

    try {
      console.log("\n=== Starting deletion of all media files from S3 ===\n");

      // Connect to database
      await sequelize.authenticate();
      console.log("✓ Database connection established");

      // Get all posts from forum_posts table
      const posts = await ForumPost.findAll({
        attributes: ["threadId", "postId"],
        raw: true,
      });

      console.log(`Found ${posts.length} posts in database`);
      console.log(`Processing in batches of ${BATCH_SIZE} posts\n`);

      let totalDeleted = 0;
      let totalFailed = 0;
      const totalBatches = Math.ceil(posts.length / BATCH_SIZE);

      // Process posts in batches
      for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        const start = batchIndex * BATCH_SIZE;
        const end = Math.min(start + BATCH_SIZE, posts.length);
        const batch = posts.slice(start, end);

        console.log(
          `\n========================================`
        );
        console.log(
          `Processing Batch ${batchIndex + 1}/${totalBatches} (Posts ${
            start + 1
          }-${end} of ${posts.length})`
        );
        console.log(
          `========================================\n`
        );

        // Process each post in the batch
        for (let i = 0; i < batch.length; i++) {
          const { threadId, postId } = batch[i];
          const globalIndex = start + i + 1;

          console.log(
            `[${globalIndex}/${posts.length}] Processing Thread: ${threadId}, Post: ${postId}`
          );

          if (threadId === 290271) continue;

          const result = await this.deletePostFiles(threadId, postId);
          totalDeleted += result.successful;
          totalFailed += result.failed;
        }

        // Batch summary
        console.log(
          `\n✓ Completed Batch ${batchIndex + 1}/${totalBatches}`
        );
        console.log(`  Total Deleted so far: ${totalDeleted}`);
        console.log(`  Total Failed so far: ${totalFailed}`);
      }

      console.log("\n========================================");
      console.log("=== Final Deletion Summary ===");
      console.log("========================================");
      console.log(`Total Files Deleted: ${totalDeleted}`);
      console.log(`Total Files Failed: ${totalFailed}`);
      console.log(`Total Posts Processed: ${posts.length}`);
      console.log(`Total Batches: ${totalBatches}`);

      // Close database connection
      await sequelize.close();
      console.log("\n✓ Database connection closed");
    } catch (error) {
      console.error("Error deleting all media:", error);
      throw error;
    }
  }
}

/**
 * Standalone function to delete S3 image files by threadId and postId
 * Can be imported and used in other services
 */
export async function deleteS3ImagesByThreadAndPost(
  threadId: number,
  postId: number
): Promise<{ successful: number; failed: number }> {
  const s3FileList = new S3FileList();
  return await s3FileList.deletePostFiles(threadId, postId);
}

// Main function - Delete ALL media files from S3 using forum_posts table
async function main() {
  const s3FileList = new S3FileList();

  try {
    // Delete ALL media files from S3 by getting post IDs from forum_posts table
    await s3FileList.deleteAllMediaFromDatabase();
  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main();
}

export { S3FileList, S3FileInfo };
