import { Controller, Get } from '@nestjs/common';

import { DataAcquisitionService } from './data-acquisition.service';

@Controller('acquisition')
export class DataAcquisitionController {
  constructor(
    private readonly dataAcquisitionService: DataAcquisitionService
  ) {}

  @Get('csv')
  getData() {
    return this.dataAcquisitionService.import_csv('test.csv', 'bank');
  }
}
