import { Entity, Column, PrimaryGeneratedColumn, ManyToOne } from 'typeorm';
import { Account } from '../account/account.entity';
import { TransactionType } from './transaction.type.enum';

@Entity()
export abstract class Transaction {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'enum', enum: TransactionType })
  type: TransactionType;

  @ManyToOne(() => Account, { nullable: false })
  account: Account;

  @Column({ type: 'date', nullable: false })
  date: Date;

  @Column({ nullable: false })
  value: number;

  @Column({ nullable: true })
  remarks: string;
}
