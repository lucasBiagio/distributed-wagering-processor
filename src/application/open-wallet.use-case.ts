import { type FinancialProcessingPort, type OpenWalletCommand, type WalletView } from './financial-contracts.js';

export class OpenWalletUseCase {
  public constructor(private readonly processing: FinancialProcessingPort) {}

  public execute(command: OpenWalletCommand): Promise<WalletView> {
    return this.processing.openWallet(command);
  }
}
