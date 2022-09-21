import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { readFile } from 'fs';
import { join } from 'path';
import { Repository } from 'typeorm';
import { Account } from './account.entity';
import { AccountType } from './account.types.enum';
import { Currency } from './currency.enum';

@Injectable()
export class AccountService {
  constructor(
    @InjectRepository(Account)
    private accountRepository: Repository<Account>
  ) {
    this.accountRepository.count().then((value) => {
      if (value == 0) {
        readFile(join('docker', 'seed_data', 'accounts.json'), (err, data) => {
          if (err) return;
          const values: [] = JSON.parse(data.toString());
          for (const val of values) {
            console.log(val);
            const account = new Account(
              val['name'],
              val['type'],
              val['currency']
            );
            this.accountRepository.save(account);
          }
        });
      }
    });
  }

  async addAccount(
    name: string,
    type: AccountType,
    currency: Currency
  ): Promise<void> {
    const account = new Account(name, type, currency);
    await this.accountRepository.save(account);
  }

  async markInactive(id: number): Promise<void> {
    await this._toggleActive(id);
  }

  async markActive(id: number): Promise<void> {
    await this._toggleActive(id);
  }

  async findAll(): Promise<Account[]> {
    return await this.accountRepository.findBy({ isActive: true });
  }

  async findOneById(id: number): Promise<Account> {
    return await this.accountRepository
      .findOneBy({ id: id, isActive: true })
      .then((account) => {
        if (account == null) {
          throw new BadRequestException('Invalid ID');
        }
        return account;
      });
  }

  async findOneByName(name: string): Promise<Account> {
    return await this.accountRepository
      .findOneBy({ name: name })
      .then((account) => {
        if (account == null) {
          throw new BadRequestException('Invalid account');
        }
        return account;
      });
  }

  async findAllByType(type: AccountType): Promise<Account[]> {
    return await this.accountRepository.findBy({ type: type, isActive: true });
  }

  async _toggleActive(id: number): Promise<void> {
    const account = await this.accountRepository.findOneBy({ id: id });
    account.isActive = !account.isActive;
    await this.accountRepository.save(account);
  }
}
