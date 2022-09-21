import { Injectable, Logger } from '@nestjs/common';
import { parse } from 'csv-parse/sync';
import { readFileSync } from 'fs';
import { CashTransaction } from '../transaction/cash.transaction.entity';
import { AccountService } from '../account/account.service';
import { TransactionService } from '../transaction/transaction.service';
import { CategoryService } from '../category/category.service';

@Injectable()
export class DataAcquisitionService {
  private readonly logger = new Logger(DataAcquisitionService.name);

  constructor(
    private accountService: AccountService,
    private categoryService: CategoryService,
    private transactionService: TransactionService
  ) {}

  async import_csv(path: string, type: string): Promise<void> {
    const input = readFileSync(path);
    //console.log(input.toString());
    const records: [] = parse(input, {
      columns: true,
      skip_empty_lines: true,
    });
    for (const record of records) {
      const cashTxn = new CashTransaction();
      cashTxn.date = new Date(record['Date']);
      cashTxn.description = record['Description'];
      cashTxn.category = await this.categoryService.findOneByLabel(
        record['Category']
      );
      cashTxn.value = record['Value'];
      cashTxn.account = await this.accountService.findOneByName(
        record['Account']
      );
      this.transactionService.addNewTransaction(cashTxn);
    }
  }
}
