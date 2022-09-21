import { Entity, Column, ManyToOne } from 'typeorm';
import { MutualFund } from '../mutual-fund/mutual-fund.entity';
import { MfTransactionType } from './mf.transaction.type.enum';
import { Transaction } from './transaction.entity';

@Entity()
export class MfTransaction extends Transaction {
  @Column({ nullable: false })
  units: number;

  @Column({ nullable: false })
  nav: number;

  @Column({ nullable: false })
  stt: number;

  @Column({ type: 'enum', enum: MfTransactionType, nullable: false })
  mfTransactionType: MfTransactionType;

  @ManyToOne(() => MutualFund, { nullable: false })
  fund: MutualFund;

  @Column({ nullable: false })
  folio: string;
}
