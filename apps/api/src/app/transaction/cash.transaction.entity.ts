import { Entity, Column, ManyToOne } from 'typeorm';
import { Category } from '../category/category.entity';
import { Transaction } from './transaction.entity';

@Entity()
export class CashTransaction extends Transaction {
  @Column({ nullable: false })
  description: string;

  @ManyToOne(() => Category, { nullable: false })
  category: Category;
}
