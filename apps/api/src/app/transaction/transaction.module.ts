import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CashTransaction } from './cash.transaction.entity';
import { MfTransaction } from './mf.transaction.entity';
import { LoanTransaction } from './loan.transaction.entity';
import { TransactionService } from './transaction.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([CashTransaction, MfTransaction, LoanTransaction]),
  ],
  providers: [TransactionService],
  exports: [TransactionService],
})
export class TransactionModule {}
