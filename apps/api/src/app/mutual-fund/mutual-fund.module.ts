import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MutualFundService } from './mutual-fund.service';
import { MutualFund } from './mutual-fund.entity';
import { MutualFundNav } from './mutual-fund.nav.entity';

@Module({
  imports: [TypeOrmModule.forFeature([MutualFund, MutualFundNav])],
  providers: [MutualFundService],
})
export class MutualFundModule {}
