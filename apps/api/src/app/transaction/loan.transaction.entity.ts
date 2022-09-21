import { Entity, Column } from 'typeorm';
import { LoanTransactionType } from './loan.transaction.type.enum';
import { Transaction } from './transaction.entity';

@Entity()
export class LoanTransaction extends Transaction {
  @Column({ type: 'enum', enum: LoanTransactionType, nullable: false })
  loanTransactionType: LoanTransactionType;

  @Column({ nullable: false })
  principal: number;

  @Column({ nullable: false })
  interest: number;
}
