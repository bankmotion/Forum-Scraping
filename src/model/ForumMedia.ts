import {
  Table,
  Column,
  Model,
  PrimaryKey,
  DataType,
  ForeignKey,
  AutoIncrement,
  Default,
} from "sequelize-typescript";
import { ForumThread } from "./ForumThread";
import { ForumPost } from "./ForumPost";

@Table({
  tableName: "forum_medias",
  timestamps: true,
})
export class ForumMedia extends Model {
  @PrimaryKey
  @AutoIncrement
  @Column(DataType.INTEGER)
  id!: number;

  @ForeignKey(() => ForumThread)
  @Column(DataType.INTEGER)
  threadId!: number;

  @ForeignKey(() => ForumPost)
  @Column(DataType.INTEGER)
  postId!: number;

  @Column(DataType.TEXT)
  link!: string;

  @Default(0)
  @Column(DataType.INTEGER)
  existThumb!: number;

  @Column({
    type: DataType.ENUM("img", "mov"),
    allowNull: true,
    defaultValue: null,
  })
  type!: "img" | "mov" | null;
}
