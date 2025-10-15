import { Table, Column, Model, PrimaryKey, DataType } from 'sequelize-typescript';

@Table({
  tableName: 'forum_threads',
  timestamps: true,
})
export class ForumThread extends Model {
  @PrimaryKey
  @Column(DataType.INTEGER)
  threadId!: number;

  @Column(DataType.TEXT)
  title!: string;

  @Column(DataType.STRING)
  creator!: string;

  @Column(DataType.STRING)
  creationDate!: string;

  @Column(DataType.STRING)
  replies!: string;

  @Column(DataType.STRING)
  views!: string;

  @Column(DataType.STRING)
  lastReplyDate!: string;

  @Column(DataType.STRING)
  lastReplier!: string;

  @Column(DataType.STRING)
  threadUrl!: string;

  @Column(DataType.INTEGER)
  lastUpdatedPage!: number;

  @Column({
    type: DataType.STRING,
    allowNull: true,
    defaultValue: null,
  })
  detailPageUpdateDate!: string | null;
}
