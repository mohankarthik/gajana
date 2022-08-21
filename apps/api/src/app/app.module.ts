import { Module } from '@nestjs/common';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';
import { ScheduleModule } from '@nestjs/schedule';
import { DataAcquisitionModule } from './data-acquisition/data-acquisition.module';

@Module({
  imports: [
    ServeStaticModule.forRoot({
      rootPath: join(__dirname, '..', 'web'),
      exclude: ['/api*'],
    }),
    ScheduleModule.forRoot(),
    DataAcquisitionModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
