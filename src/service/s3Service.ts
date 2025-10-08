import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import * as https from "https";
import * as http from "http";
import * as fs from "fs";
import * as path from "path";
import * as zlib from "zlib";

import dotenv from "dotenv";

interface UploadTask {
  url: string;
  key: string;
  threadId: number;
  postId: number;
  retryCount?: number;
}

interface UploadStats {
  total: number;
  successful: number;
  failed: number;
  skipped: number;
  startTime: number;
  endTime?: number;
}

export class S3Service {
  s3Client: S3Client;
  bucketName: string;
  private uploadQueue: UploadTask[] = [];
  private activeUploads: Set<string> = new Set();
  private uploadStats: UploadStats = {
    total: 0,
    successful: 0,
    failed: 0,
    skipped: 0,
    startTime: 0
  };

  // Configuration for concurrent uploads
  private readonly MAX_CONCURRENT_UPLOADS = parseInt(process.env.MAX_CONCURRENT_UPLOADS || "64");
  private readonly MAX_RETRIES = 3;
  private readonly RETRY_DELAY = 2000; // 2 seconds
  private readonly DOWNLOAD_TIMEOUT = 30000; // 30 seconds

  constructor() {
    this.bucketName = process.env.S3_BUCKET_NAME || "";

    // Fix the endpoint URL by adding protocol
    const endpoint = process.env.S3_ENDPOINT || "";
    const fullEndpoint = endpoint.startsWith("http")
      ? endpoint
      : `https://${endpoint}`;

    // Create a custom HTTPS agent that ignores SSL certificate errors
    const httpsAgent = new https.Agent({
      rejectUnauthorized: false,
      keepAlive: true,
      maxSockets: this.MAX_CONCURRENT_UPLOADS * 2, // Allow more connections
    });

    this.s3Client = new S3Client({
      region: process.env.S3_REGION || "us-east-2",
      endpoint: fullEndpoint,
      credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY_ID || "",
        secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || "",
      },
      forcePathStyle: true, // Required for Wasabi
      requestHandler: {
        httpsAgent: httpsAgent,
      },
      maxAttempts: 3, // AWS SDK retry attempts
    });
  }

  async uploadFromUrl(sourceUrl: string, key: string): Promise<string> {
    try {
      // Download the file from the source URL
      const fileBuffer = await this.downloadFile(sourceUrl);

      // Determine content type based on file extension
      const contentType = this.getContentType(sourceUrl);

      // Upload to S3 bucket
      const uploadCommand = new PutObjectCommand({
        Bucket: this.bucketName,
        Key: key,
        Body: fileBuffer,
        ContentType: contentType,
        Metadata: {
          "original-url": sourceUrl,
          "upload-timestamp": new Date().toISOString(),
        },
      });

      await this.s3Client.send(uploadCommand);

      // Generate S3 URL
      const s3Url = `https://${this.bucketName}.s3.${process.env.S3_REGION}.wasabisys.com/${key}`;

      console.log(`Successfully uploaded to S3: ${s3Url}`);

      return s3Url;
    } catch (error) {
      console.error(`Error uploading ${sourceUrl} to S3:`, error);
      return "";
    }
  }

  /**
   * Upload multiple media URLs concurrently with queue management
   */
  async uploadMediaUrls(
    medias: string[],
    threadId: number,
    postId: number
  ): Promise<string[]> {
    console.log(`Starting concurrent upload of ${medias.length} media files for post ${postId}`);
    
    // Reset stats
    this.uploadStats = {
      total: medias.length,
      successful: 0,
      failed: 0,
      skipped: 0,
      startTime: Date.now()
    };

    // Filter and prepare upload tasks
    const uploadTasks: UploadTask[] = [];
    for (const mediaUrl of medias) {
      if (this.isImageOrVideo(mediaUrl)) {
        const key = this.generateKey(mediaUrl, threadId, postId);
        uploadTasks.push({
          url: mediaUrl,
          key,
          threadId,
          postId,
          retryCount: 0
        });
      } else {
        this.uploadStats.skipped++;
      }
    }

    // Process uploads concurrently
    const uploadedUrls = await this.processConcurrentUploads(uploadTasks);
    
    // Log final stats
    this.uploadStats.endTime = Date.now();
    const duration = this.uploadStats.endTime - this.uploadStats.startTime;
    const rate = this.uploadStats.successful / (duration / 1000);
    
    console.log(`Upload completed: ${this.uploadStats.successful}/${this.uploadStats.total} successful, ${this.uploadStats.failed} failed, ${this.uploadStats.skipped} skipped`);
    console.log(`Upload rate: ${rate.toFixed(2)} files/second, Duration: ${(duration/1000).toFixed(2)}s`);

    return uploadedUrls;
  }

  /**
   * Process uploads concurrently with proper queue management
   */
  private async processConcurrentUploads(tasks: UploadTask[]): Promise<string[]> {
    const results: string[] = [];
    const promises: Promise<void>[] = [];

    // Process tasks in batches
    for (let i = 0; i < tasks.length; i += this.MAX_CONCURRENT_UPLOADS) {
      const batch = tasks.slice(i, i + this.MAX_CONCURRENT_UPLOADS);
      
      const batchPromises = batch.map(async (task) => {
        try {
          const result = await this.processUploadTask(task);
          if (result) {
            results.push(result);
            this.uploadStats.successful++;
          } else {
            this.uploadStats.failed++;
          }
        } catch (error) {
          console.error(`Failed to process upload task for ${task.url}:`, error);
          this.uploadStats.failed++;
        }
      });

      promises.push(...batchPromises);
      
      // Add small delay between batches to prevent overwhelming the system
      if (i + this.MAX_CONCURRENT_UPLOADS < tasks.length) {
        await this.delay(100);
      }
    }

    await Promise.allSettled(promises);
    return results;
  }

  /**
   * Process individual upload task with retry logic
   */
  private async processUploadTask(task: UploadTask): Promise<string | null> {
    const taskId = `${task.threadId}-${task.postId}-${task.key}`;
    
    try {
      // Check if already processing this task
      if (this.activeUploads.has(taskId)) {
        console.log(`Task ${taskId} already in progress, skipping`);
        return null;
      }

      this.activeUploads.add(taskId);

      // Download and upload
      const fileBuffer = await this.downloadFile(task.url);
      const contentType = this.getContentType(task.url);

      const uploadCommand = new PutObjectCommand({
        Bucket: this.bucketName,
        Key: task.key,
        Body: fileBuffer,
        ContentType: contentType,
        Metadata: {
          "original-url": task.url,
          "upload-timestamp": new Date().toISOString(),
          "thread-id": task.threadId.toString(),
          "post-id": task.postId.toString(),
        },
      });

      await this.s3Client.send(uploadCommand);

      const s3Url = `https://${this.bucketName}.s3.${process.env.S3_REGION}.wasabisys.com/${task.key}`;
      
      return s3Url;

    } catch (error) {
      console.error(`Upload failed for ${task.url}:`, error);
      
      // Retry logic
      if (task.retryCount! < this.MAX_RETRIES) {
        task.retryCount!++;
        console.log(`Retrying upload for ${task.url} (attempt ${task.retryCount})`);
        
        await this.delay(this.RETRY_DELAY * task.retryCount!);
        return this.processUploadTask(task);
      }
      
      return null;
    } finally {
      this.activeUploads.delete(taskId);
    }
  }

  /**
   * Enhanced download with better error handling and timeout
   */
  public async downloadFile(url: string): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const parsedUrl = new URL(url);
      
      const options = {
        timeout: this.DOWNLOAD_TIMEOUT,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate',
          'Connection': 'keep-alive'
        }
      };

      const request =
        parsedUrl.protocol === "https:"
          ? https.get(url, options, (response) => {
              this.handleResponse(response, resolve, reject);
            })
          : http.get(url, options, (response) => {
              this.handleResponse(response, resolve, reject);
            });

      request.on("error", (error) => {
        console.error(`Download error for ${url}:`, error);
        reject(error);
      });
      
      request.setTimeout(this.DOWNLOAD_TIMEOUT, () => {
        request.destroy();
        reject(new Error(`Download timeout for ${url}`));
      });
    });
  }

  private handleResponse(
    response: any,
    resolve: Function,
    reject: Function
  ): void {
    if (response.statusCode !== 200) {
      reject(new Error(`Failed to download file: ${response.statusCode}`));
      return;
    }

    const chunks: Buffer[] = [];
    
    // Check if the response is gzipped
    const contentEncoding = response.headers['content-encoding'];
    let stream = response;
    
    if (contentEncoding === 'gzip') {
      stream = response.pipe(zlib.createGunzip());
    } else if (contentEncoding === 'deflate') {
      stream = response.pipe(zlib.createInflate());
    }
    
    stream.on("data", (chunk: Buffer) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  }

  /**
   * Check if URL is an image or video (public method)
   */
  isImageOrVideo(url: string): boolean {
    const extension = url.split(".").pop()?.toLowerCase();

    const imageExtensions = [
      "jpg",
      "jpeg",
      "png",
      "gif",
      "webp",
      "bmp",
      "svg",
      "tiff",
      "ico",
    ];
    const videoExtensions = [
      "mp4",
      "webm",
      "mov",
      "avi",
      "mkv",
      "flv",
      "wmv",
      "m4v",
      "3gp",
      "ogv",
    ];

    return (
      imageExtensions.includes(extension || "") ||
      videoExtensions.includes(extension || "")
    );
  }

  /**
   * Generate S3 key (public method)
   */
  generateKey(
    originalUrl: string,
    threadId: number,
    postId: number,
    isThumbnail: boolean = false,
    uniqueId: number = 0
  ): string {
    const url = new URL(originalUrl);
    const pathParts = url.pathname.split("/");
    const filename = pathParts[pathParts.length - 1] || "media";
    const extension = filename.split(".").pop() || "jpg";
    
    // Remove extension from filename to add _thumb before extension
    let baseFilename = filename.replace(`.${extension}`, "");
    
    // Remove "-media" from the filename for full images, but keep it for thumbnails
    if (!isThumbnail && baseFilename.includes("-media")) {
      baseFilename = baseFilename.replace("-media", "");
    }

    // Generate unique key: forum-media/threadId/postId/uniqueId-filename[_thumb].extension
    const thumbSuffix = isThumbnail ? "_thumb" : "";
    return `forum-media/${threadId}/${postId}/${uniqueId}-${baseFilename}${thumbSuffix}.${extension}`;
  }

  generateKeyWithExtension(
    originalUrl: string,
    threadId: number,
    postId: number,
    extension: string,
    isThumbnail: boolean = false,
    uniqueId: number = 0
  ): string {
    // const url = new URL(originalUrl);
    const pathParts = originalUrl.split("/");
    const filename = pathParts[pathParts.length - 1] || "media";
    
    // Remove any existing extension and add the specified one
    let baseFilename = filename.split(".")[0] || "media";
    
    // Remove "-media" from the filename for full images, but keep it for thumbnails
    if (!isThumbnail && baseFilename.includes("-media")) {
      baseFilename = baseFilename.replace("-media", "");
    }
    
    const sanitizedFilename = baseFilename.replace(/[^a-zA-Z0-9.-]/g, "_");
    
    // Ensure extension starts with dot
    const normalizedExtension = extension.startsWith('.') ? extension : `.${extension}`;
    
    // Generate unique key: forum-media/threadId/postId/uniqueId-filename[_thumb].extension
    const thumbSuffix = isThumbnail ? "_thumb" : "";
    return `forum-media/${threadId}/${postId}/${uniqueId}-${sanitizedFilename}${thumbSuffix}${normalizedExtension}`;
  }

  /**
   * Get content type (public method)
   */
  getContentType(url: string): string {
    const extension = url.split(".").pop()?.toLowerCase();

    switch (extension) {
      case "jpg":
      case "jpeg":
        return "image/jpeg";
      case "png":
        return "image/png";
      case "gif":
        return "image/gif";
      case "webp":
        return "image/webp";
      case "mp4":
        return "video/mp4";
      case "webm":
        return "video/webm";
      case "mov":
        return "video/quicktime";
      case "avi":
        return "video/x-msvideo";
      default:
        return "application/octet-stream";
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get current upload statistics
   */
  getUploadStats(): UploadStats {
    return { ...this.uploadStats };
  }

  /**
   * Reset upload statistics
   */
  resetUploadStats(): void {
    this.uploadStats = {
      total: 0,
      successful: 0,
      failed: 0,
      skipped: 0,
      startTime: 0
    };
  }
}
