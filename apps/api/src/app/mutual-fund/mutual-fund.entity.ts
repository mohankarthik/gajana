import { Entity, Column, PrimaryColumn } from 'typeorm';

@Entity()
export abstract class MutualFund {
  @PrimaryColumn({ unique: true })
  id: number;

  @Column({ nullable: false })
  type: string;

  @Column({ nullable: false })
  category: string;

  @Column({ nullable: false, unique: true })
  name: string;
}
