import { Entity, Column, PrimaryColumn, ManyToOne } from 'typeorm';
import { MutualFund } from './mutual-fund.entity';

@Entity()
export abstract class MutualFundNav {
  @PrimaryColumn({ unique: true })
  id: number;

  @ManyToOne(() => MutualFund, { nullable: false })
  fund: MutualFund;

  @Column({ nullable: false, type: 'date' })
  date: Date;

  @Column({ nullable: false })
  nav: number;
}
