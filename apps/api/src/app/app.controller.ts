import { Controller, Get } from '@nestjs/common';

import { AppService } from './app.service';
import { DataAcquisitionGmailService } from './data-acquisition/data-acquisition-gmail.service';

@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
    private readonly dataAcquisitionGmailSerivce: DataAcquisitionGmailService
  ) {}

  @Get()
  getData() {
    //return this.appService.getData();
    return this.dataAcquisitionGmailSerivce.getLabels();
  }
}
