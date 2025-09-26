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
import { URL } from "url";
import dotenv from "dotenv";

dotenv.config();

export class S3Service {
  private s3Client: S3Client;
  private bucketName: string;

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
        httpsAgent: httpsAgent, // Use custom agent for S3 requests
      },
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

  private async downloadFile(url: string): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const parsedUrl = new URL(url);

      // Create a custom agent that ignores SSL certificate errors
      const agent = new https.Agent({
        rejectUnauthorized: false,
        timeout: 30000,
      });

      const options: any = {
        agent: parsedUrl.protocol === "https:" ? agent : undefined,
        timeout: 30000,
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        },
      };

      const request =
        parsedUrl.protocol === "https:"
          ? https.get(url, options, (response) => {
              this.handleResponse(response, resolve, reject);
            })
          : http.get(url, options, (response) => {
              this.handleResponse(response, resolve, reject);
            });

      request.on("error", reject);
      request.setTimeout(30000, () => {
        request.destroy();
        reject(new Error("Download timeout"));
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
    response.on("data", (chunk: Buffer) => chunks.push(chunk));
    response.on("end", () => resolve(Buffer.concat(chunks)));
    response.on("error", reject);
  }

  private getContentType(url: string): string {
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

  private generateKey(
    originalUrl: string,
    threadId: string,
    postId: number
  ): string {
    const url = new URL(originalUrl);
    const pathParts = url.pathname.split("/");
    const filename = pathParts[pathParts.length - 1] || "media";
    const extension = filename.split(".").pop() || "jpg";

    // Generate unique key: forum-media/threadId/postId/timestamp-filename
    const timestamp = Date.now();
    return `forum-media/${threadId}/${postId}/${timestamp}-${filename}`;
  }

  private isImageOrVideo(url: string): boolean {
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

  async uploadMediaUrls(
    medias: string[],
    threadId: string,
    postId: number
  ): Promise<string[]> {
    const uploadedUrls: string[] = [];
    let skippedCount = 0;

    for (const mediaUrl of medias) {
      try {
        // Check if the file is an image or video
        if (!this.isImageOrVideo(mediaUrl)) {
          skippedCount++;
          continue;
        }

        const key = this.generateKey(mediaUrl, threadId, postId);
        const uploadedMediaUrl = await this.uploadFromUrl(mediaUrl, key);
        if (uploadedMediaUrl) {
          uploadedUrls.push(uploadedMediaUrl);
        }

        // Add delay between downloads to avoid rate limiting
        await this.delay(1000);
      } catch (error) {
        console.error(`Failed to download ${mediaUrl}:`, error);
        // Keep original URL if download fails
        uploadedUrls.push(mediaUrl);
      }
    }
    return uploadedUrls;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
