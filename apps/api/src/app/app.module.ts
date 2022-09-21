import { Module } from '@nestjs/common';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';
import { ScheduleModule } from '@nestjs/schedule';
import { DataAcquisitionModule } from './data-acquisition/data-acquisition.module';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AccountModule } from './account/account.module';
import { CategoryModule } from './category/category.module';
import { MutualFundModule } from './mutual-fund/mutual-fund.module';
import { TransactionModule } from './transaction/transaction.module';

@Module({
  imports: [
    ServeStaticModule.forRoot({
      rootPath: join(__dirname, '..', 'web'),
      exclude: ['/api*'],
    }),
    TypeOrmModule.forRoot({
      type: 'mysql',
      host: 'localhost',
      port: 16000,
      username: 'root',
      password: 'admin',
      database: 'gajana',
      synchronize: true,
      autoLoadEntities: true,
      retryAttempts: 1,
    }),
    ScheduleModule.forRoot(),
    DataAcquisitionModule,
    AccountModule,
    CategoryModule,
    MutualFundModule,
    TransactionModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
