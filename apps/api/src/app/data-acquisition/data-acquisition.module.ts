import { Module } from '@nestjs/common';
import { AccountModule } from '../account/account.module';
import { CategoryModule } from '../category/category.module';
import { TransactionModule } from '../transaction/transaction.module';
import { DataAcquisitionController } from './data-acquisition.controller';
import { DataAcquisitionService } from './data-acquisition.service';

@Module({
  imports: [AccountModule, CategoryModule, TransactionModule],
  providers: [DataAcquisitionService],
  controllers: [DataAcquisitionController],
})
export class DataAcquisitionModule {}
