import { type FinancialProcessingPort, type ProcessBetCommand, type ProcessWagerResult } from './financial-contracts.js';

export class ProcessWagerUseCase {
  public constructor(private readonly processing: FinancialProcessingPort) {}

  public execute(command: ProcessBetCommand): Promise<ProcessWagerResult> {
    return this.processing.processBet(command);
  }
}
