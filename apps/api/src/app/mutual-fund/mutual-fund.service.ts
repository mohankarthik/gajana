import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { from, Observable } from 'rxjs';
import { Repository } from 'typeorm';
import { MutualFund } from './mutual-fund.entity';
import { MutualFundNav } from './mutual-fund.nav.entity';

@Injectable()
export class MutualFundService {
  constructor(
    @InjectRepository(MutualFund)
    private mutualFundRepository: Repository<MutualFund>,

    @InjectRepository(MutualFundNav)
    private mutualFundNavRepository: Repository<MutualFundNav>
  ) {}

  findAll(): Observable<MutualFund[]> {
    return from(this.mutualFundRepository.find());
  }

  findAllByType(type: string): Observable<MutualFund[]> {
    return from(this.mutualFundRepository.findBy({ type: type }));
  }

  findOneById(id: number): Observable<MutualFund> {
    return from(this.mutualFundRepository.findOneBy({ id: id }));
  }

  findOneByName(name: string): Observable<MutualFund> {
    return from(this.mutualFundRepository.findOneBy({ name: name }));
  }

  getLatestNav(fund: MutualFund): Observable<MutualFundNav> {
    return from(
      this.mutualFundNavRepository.findOne({
        where: [{ fund: fund }],
        order: { date: 'DESC' },
      })
    );
  }
}
