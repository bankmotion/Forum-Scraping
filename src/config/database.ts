import dotenv from "dotenv";
import { Sequelize } from 'sequelize-typescript';

dotenv.config();

console.log(process.env.DB_NAME);

export const config = {
    database: process.env.DB_NAME || "form_scrp",
    username: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD || "",
    host: process.env.DB_HOST || "localhost",
    port: parseInt(process.env.DB_PORT || "3306"),
    dialect: "mysql" as const,
    logging: false,
    models: [__dirname + "/../model/*.ts"],
};

// Create and export the sequelize instance
export const sequelize = new Sequelize(config);
  