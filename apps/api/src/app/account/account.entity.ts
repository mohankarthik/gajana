import { Entity, Column, PrimaryGeneratedColumn } from 'typeorm';
import { AccountType } from './account.types.enum';
import { Currency } from './currency.enum';

@Entity()
export class Account {
  constructor();
  constructor(name: string, type: AccountType, currency: Currency);
  constructor(name?: string, type?: AccountType, currency?: Currency) {
    this.name = name;
    this.type = type;
    this.currency = currency;
  }

  @PrimaryGeneratedColumn()
  id: number;

  @Column({ nullable: false })
  name: string;

  @Column({ nullable: false, type: 'enum', enum: AccountType })
  type: AccountType;

  @Column({ nullable: false, default: true })
  isActive: boolean;

  @Column({ nullable: false, type: 'enum', enum: Currency })
  currency: Currency;
}
