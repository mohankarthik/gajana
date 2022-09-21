import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Category } from './category.entity';

@Injectable()
export class CategoryService {
  constructor(
    @InjectRepository(Category)
    private categoryRepository: Repository<Category>
  ) {}

  async addCategory(label: string): Promise<void> {
    await this.categoryRepository.save(new Category(label));
  }

  async renameCategory(id: number, newLabel: string): Promise<void> {
    const category = await this.categoryRepository.findOneBy({ id: id });
    category.label = newLabel;
    await this.categoryRepository.save(category);
  }

  async deleteCategory(id: number): Promise<void> {
    await this.categoryRepository.delete({ id: id });
  }

  async findAll(): Promise<Category[]> {
    return await this.categoryRepository.find();
  }

  async findOneById(id: number): Promise<Category> {
    const category = await this.categoryRepository.findOneBy({ id: id });
    if (category == null) {
      throw new BadRequestException('Invalid Category');
    }
    return category;
  }

  async findOneByLabel(label: string): Promise<Category> {
    const category = await this.categoryRepository.findOneBy({ label: label });
    if (category == null) {
      throw new BadRequestException('Invalid Category');
    }
    return category;
  }
}
