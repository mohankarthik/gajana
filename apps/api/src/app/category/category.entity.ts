import { Entity, Column, PrimaryGeneratedColumn } from 'typeorm';

@Entity()
export class Category {
  constructor(label: string) {
    this.label = label;
  }

  @PrimaryGeneratedColumn()
  id: number;

  @Column({ nullable: false, unique: true })
  label: string;
}
