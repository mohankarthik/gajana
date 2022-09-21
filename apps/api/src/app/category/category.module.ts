import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CategoryService } from './category.service';
import { Category } from './category.entity';

@Module({
  imports: [TypeOrmModule.forFeature([Category])],
  providers: [CategoryService],
  exports: [CategoryService],
})
export class CategoryModule {}
