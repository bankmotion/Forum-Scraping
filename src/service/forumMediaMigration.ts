import { ForumPost } from "../model/ForumPost";
import { ForumMedia } from "../model/ForumMedia";
import { sequelize } from "../config/database";
import { Op } from "sequelize";

interface MediaData {
  threadId: number;
  postId: number;
  link: string;
  type: "img" | "mov" | null;
}

class ForumMediaMigration {
  private batchSize: number = 100;

  constructor() {
    this.batchSize = parseInt(process.env.BATCH_SIZE || "100");
  }

  /**
   * Determine media type based on file extension
   */
  private getMediaType(url: string): "img" | "mov" | null {
    const lowerUrl = url.toLowerCase();

    // Image extensions
    const imageExtensions = [
      ".jpg",
      ".jpeg",
      ".png",
      ".gif",
      ".bmp",
      ".webp",
      ".svg",
    ];
    // Video/movie extensions
    const videoExtensions = [
      ".mp4",
      ".mov",
      ".avi",
      ".mkv",
      ".webm",
      ".flv",
      ".wmv",
      ".m4v",
    ];

    for (const ext of imageExtensions) {
      if (lowerUrl.includes(ext)) {
        return "img";
      }
    }

    for (const ext of videoExtensions) {
      if (lowerUrl.includes(ext)) {
        return "mov";
      }
    }

    return null;
  }

  /**
   * Parse JSON string and extract media links
   */
  private parseMediasJson(mediasJson: string): string[] {
    try {
      if (!mediasJson || mediasJson.trim() === "") {
        return [];
      }

      const parsed = JSON.parse(mediasJson);

      // Handle different JSON structures
      if (Array.isArray(parsed)) {
        return parsed.filter(
          (link) => typeof link === "string" && link.trim() !== ""
        );
      }

      if (typeof parsed === "object" && parsed !== null) {
        // If it's an object, try to extract links from common properties
        const links: string[] = [];
        Object.values(parsed).forEach((value) => {
          if (typeof value === "string" && value.trim() !== "") {
            links.push(value);
          } else if (Array.isArray(value)) {
            links.push(
              ...value.filter(
                (link) => typeof link === "string" && link.trim() !== ""
              )
            );
          }
        });
        return links;
      }

      return [];
    } catch (error) {
      console.error("Error parsing medias JSON:", error);
      console.error("Problematic JSON:", mediasJson);
      return [];
    }
  }

  /**
   * Get all forum posts (since we now store media separately in ForumMedia table)
   */
  private async getAllPosts(): Promise<ForumPost[]> {
    try {
      console.log("Fetching all posts...");

      const posts = await ForumPost.findAll({
        order: [["postId", "ASC"]],
      });

      console.log(`Found ${posts.length} posts`);
      return posts;
    } catch (error) {
      console.error("Error fetching posts:", error);
      throw error;
    }
  }

  /**
   * Process a single post and create media records
   * Note: Since medias field is removed from ForumPost, this method now returns empty array
   * Media data should be managed directly through ForumMedia table
   */
  private async processPost(post: ForumPost): Promise<MediaData[]> {
    // Since we removed the medias field from ForumPost, this method now returns empty array
    // Media data is now stored directly in ForumMedia table
    console.log(`Post ${post.postId} has no embedded media data (using ForumMedia table instead)`);
    return [];
  }

  /**
   * Save media data to database
   */
  private async saveMediaData(mediaData: MediaData[]): Promise<void> {
    if (mediaData.length === 0) {
      return;
    }

    try {
      // Use bulkCreate for better performance
      await ForumMedia.bulkCreate(mediaData as any, {
        ignoreDuplicates: true, // Skip duplicates if any
        validate: true,
      });

      console.log(`Saved ${mediaData.length} media records`);
    } catch (error) {
      console.error("Error saving media data:", error);
      throw error;
    }
  }

  /**
   * Get count of existing media records
   */
  private async getExistingMediaCount(): Promise<number> {
    try {
      const count = await ForumMedia.count();
      return count;
    } catch (error) {
      console.error("Error getting existing media count:", error);
      return 0;
    }
  }

  /**
   * Main migration function
   */
  async migrateMedias(): Promise<void> {
    try {
      console.log("Starting forum media migration...");

      // Check existing media count
      const existingCount = await this.getExistingMediaCount();
      console.log(`Existing media records: ${existingCount}`);

      // Get all posts
      const posts = await this.getAllPosts();

      if (posts.length === 0) {
        console.log("No posts with media data found. Migration completed.");
        return;
      }

      let totalProcessed = 0;
      let totalMediaRecords = 0;
      let errorCount = 0;

      // Process posts in batches
      for (let i = 0; i < posts.length; i += this.batchSize) {
        const batch = posts.slice(i, i + this.batchSize);
        console.log(
          `Processing batch ${Math.floor(i / this.batchSize) + 1}/${Math.ceil(
            posts.length / this.batchSize
          )} (${batch.length} posts)...`
        );

        const batchMediaData: MediaData[] = [];

        for (const post of batch) {
          try {
            const mediaData = await this.processPost(post);
            batchMediaData.push(...mediaData);
            totalProcessed++;

            if (totalProcessed % 50 === 0) {
              console.log(
                `Processed ${totalProcessed}/${posts.length} posts...`
              );
            }
          } catch (error) {
            console.error(`Error processing post ${post.postId}:`, error);
            errorCount++;
          }
        }

        // Save batch media data
        if (batchMediaData.length > 0) {
          try {
            await this.saveMediaData(batchMediaData);
            totalMediaRecords += batchMediaData.length;
          } catch (error) {
            console.error(`Error saving batch media data:`, error);
            errorCount++;
          }
        }

        // Add a small delay between batches to avoid overwhelming the database
        if (i + this.batchSize < posts.length) {
          await this.delay(100);
        }
      }

      console.log("\n=== Migration Summary ===");
      console.log(`Total posts processed: ${totalProcessed}`);
      console.log(`Total media records created: ${totalMediaRecords}`);
      console.log(`Errors encountered: ${errorCount}`);
      console.log(
        `Final media records in database: ${await this.getExistingMediaCount()}`
      );

      if (errorCount > 0) {
        console.log(
          "\n⚠️  Some errors occurred during migration. Check the logs above for details."
        );
      } else {
        console.log("\n✅ Migration completed successfully!");
      }
    } catch (error) {
      console.error("Migration failed:", error);
      throw error;
    }
  }

  /**
   * Utility function to add delay
   */
  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Test function to preview what would be migrated
   */
  async previewMigration(limit: number = 10): Promise<void> {
    try {
      console.log(`Previewing migration for first ${limit} posts...`);

      const posts = await ForumPost.findAll({
        limit: limit,
        order: [["postId", "ASC"]],
      });

      console.log(`\nFound ${posts.length} posts:\n`);

      for (const post of posts) {
        console.log(`Post ID: ${post.postId}, Thread ID: ${post.threadId}`);
        console.log(`Content: ${post.content.substring(0, 100)}...`);

        const mediaData = await this.processPost(post);
        console.log(`Media data (from ForumMedia table): ${mediaData.length} records`);

        console.log("---");
      }
    } catch (error) {
      console.error("Preview failed:", error);
      throw error;
    }
  }
}

// Export the class
export { ForumMediaMigration };

// Run migration if this file is executed directly
if (require.main === module) {
  const migration = new ForumMediaMigration();

  // Check command line arguments
  const args = process.argv.slice(2);

  if (args.includes("--preview")) {
    const limit = args.find((arg) => arg.startsWith("--limit="))?.split("=")[1];
    migration
      .previewMigration(limit ? parseInt(limit) : 10)
      .then(() => process.exit(0))
      .catch((error) => {
        console.error(error);
        process.exit(1);
      });
  } else {
    migration
      .migrateMedias()
      .then(() => process.exit(0))
      .catch((error) => {
        console.error(error);
        process.exit(1);
      });
  }
}
