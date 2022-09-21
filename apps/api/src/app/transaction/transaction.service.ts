import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { from, merge, Observable } from 'rxjs';
import { Repository } from 'typeorm';
import { Category } from '../category/category.entity';
import { CashTransaction } from './cash.transaction.entity';
import { LoanTransaction } from './loan.transaction.entity';
import { MfTransaction } from './mf.transaction.entity';
import { Transaction } from './transaction.entity';
import { TransactionType } from './transaction.type.enum';

@Injectable()
export class TransactionService {
  constructor(
    @InjectRepository(CashTransaction)
    private cashTransactionRepository: Repository<CashTransaction>,

    @InjectRepository(MfTransaction)
    private mutualFundTransactionRepository: Repository<MfTransaction>,

    @InjectRepository(LoanTransaction)
    private loanTransactionRepository: Repository<LoanTransaction>
  ) {}

  addNewTransaction(
    transaction: CashTransaction | MfTransaction | LoanTransaction
  ) {
    if (transaction instanceof CashTransaction) {
      this.cashTransactionRepository.save(transaction);
    } else if (transaction instanceof MfTransaction) {
      this.mutualFundTransactionRepository.save(transaction);
    } else if (transaction instanceof LoanTransaction) {
      this.loanTransactionRepository.save(transaction);
    } else {
      throw Error(`Can't save unknown transaction ${transaction}`);
    }
  }

  async updateCategory(id: number, type: TransactionType, category: Category) {
    if (type == TransactionType.cash) {
      const txn = await this.cashTransactionRepository.findOneBy({ id: id });
      if (txn == null)
        throw new BadRequestException(`Invalid transaction ${id}`);
      txn.category = category;
      this.cashTransactionRepository.save(txn);
    } else {
      throw new BadRequestException(
        `Category change not supported for loan transactions`
      );
    }
  }

  findAll(): Observable<Transaction[]> {
    return merge(
      from(this.cashTransactionRepository.find()),
      from(this.mutualFundTransactionRepository.find()),
      from(this.loanTransactionRepository.find())
    );
  }
}
