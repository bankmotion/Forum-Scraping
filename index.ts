import "reflect-metadata";
import { sequelize } from "./src/config/database";
import { ForumScraper } from "./src/service/forumScrapingScript";
import { ForumDetailPageScraper } from "./src/service/forumDetailPageScraping";
import { ForumMediaMigration } from "./src/service/forumMediaMigration";
import { S3FileList } from "./src/service/s3FileList";
import { ForumThreadLatestUpdateChecker } from "./src/service/forumThreadLatestUpdate";

async function main() {
  try {
    // Test connection
    await sequelize.authenticate();
    console.log("Database connection established successfully.");

    // Sync database (create tables if they don't exist)
    await sequelize.sync();
    console.log("Database synchronized successfully.");

    // Run the forum scraper
    // console.log('Starting forum scraping...');
    // const scraper = new ForumScraper();
    // await scraper.run();

    // console.log('Starting forum thread latest update...');
    // const forumThreadLatestUpdate = new ForumThreadLatestUpdateChecker();
    // await forumThreadLatestUpdate.run();

    // // console.log('Starting detail page scraping...');
    const detailPageScraper = new ForumDetailPageScraper();
    await detailPageScraper.run();

    // const threadId = 10003071;
    // const threadId = 7400871;
    // const detailPageScraper = new ForumDetailPageScraper();
    // const detailPage = await detailPageScraper.runDetailPage(threadId);
    // console.log(detailPage);

    // console.log("Starting forum media migration...");
    // const forumMediaMigration = new ForumMediaMigration();
    // await forumMediaMigration.migrateMedias();

    // run getting file via s3FileList
    // const s3FileList = new S3FileList();
    // const files = await s3FileList.deleteAllMediaFromDatabase();
    // console.log(files);
  } catch (error) {
    console.error("Error in main execution:", error);
  }
}

main();
