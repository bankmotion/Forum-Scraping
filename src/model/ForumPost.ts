import { Table, Column, Model, PrimaryKey, DataType, ForeignKey } from 'sequelize-typescript';
import { ForumThread } from './ForumThread';

@Table({
  tableName: 'forum_posts',
  timestamps: true,
})
export class ForumPost extends Model {
  @PrimaryKey
  @Column(DataType.INTEGER)
  postId!: number;

  @ForeignKey(() => ForumThread)
  @Column(DataType.INTEGER)
  threadId!: number;

  @Column(DataType.STRING)
  author!: string;

  @Column(DataType.TEXT)
  content!: string;

  @Column(DataType.STRING)
  postCreatedDate!: string;

  @Column(DataType.STRING)
  likes!: string;
} 