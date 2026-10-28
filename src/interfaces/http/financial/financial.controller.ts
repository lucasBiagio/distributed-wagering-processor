import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  Headers,
  HttpCode,
  NotFoundException,
  Param,
  Post,
  UnprocessableEntityException,
} from '@nestjs/common';

import { FinancialConflictError, FinancialNotFoundError, type OpenWalletCommand, type ProcessBetCommand } from '../../../application/financial-contracts.js';
import { DomainRuleError } from '../../../domain/domain-error.js';
import { CurrencyMismatchError, MoneyValidationError } from '../../../domain/money.js';
import { MikroOrmFinancialProcessing } from '../../../infrastructure/persistence/mikro-orm-financial-processing.js';

function object(value: unknown, field: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new BadRequestException({ failureCode: 'INVALID_PAYLOAD', message: `${field} must be an object` });
  }
  return value as Record<string, unknown>;
}

function string(value: unknown, field: string): string {
  if (typeof value !== 'string') {
    throw new BadRequestException({ failureCode: 'INVALID_PAYLOAD', message: `${field} must be a string` });
  }
  return value;
}

function money(value: unknown): { amount: string; currency: string } {
  const candidate = object(value, 'money');
  return { amount: string(candidate.amount, 'money.amount'), currency: string(candidate.currency, 'money.currency') };
}

@Controller()
export class FinancialController {
  public constructor(private readonly processing: MikroOrmFinancialProcessing) {}

  @Post('wallets')
  public async openWallet(@Body() body: unknown): Promise<unknown> {
    const payload = object(body, 'body');
    try {
      return await this.processing.openWallet({
        playerId: string(payload.playerId, 'playerId'),
        initialBalance: money(payload.initialBalance),
      } satisfies OpenWalletCommand);
    } catch (error) {
      this.rethrow(error);
    }
  }

  @Get('wallets/:walletId')
  public async getWallet(@Param('walletId') walletId: string): Promise<unknown> {
    const wallet = await this.processing.getWallet(walletId);
    if (!wallet) {
      throw new NotFoundException({ failureCode: 'WALLET_NOT_FOUND' });
    }
    return wallet;
  }

  @Post('wagering/transactions')
  @HttpCode(200)
  public async processWager(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
  ): Promise<unknown> {
    const payload = object(body, 'body');
    if (!idempotencyKey) {
      throw new BadRequestException({ failureCode: 'IDEMPOTENCY_KEY_REQUIRED' });
    }
    try {
      return await this.processing.processBet({
        idempotencyKey,
        providerId: string(payload.providerId, 'providerId'),
        externalTransactionId: string(payload.externalTransactionId, 'externalTransactionId'),
        walletId: string(payload.walletId, 'walletId'),
        playerId: string(payload.playerId, 'playerId'),
        roundId: string(payload.roundId, 'roundId'),
        gameId: string(payload.gameId, 'gameId'),
        kind: string(payload.kind, 'kind') as 'BET',
        money: money(payload.money),
        ...(payload.referenceExternalTransactionId === undefined
          ? {}
          : { referenceExternalTransactionId: string(payload.referenceExternalTransactionId, 'referenceExternalTransactionId') }),
      } satisfies ProcessBetCommand);
    } catch (error) {
      this.rethrow(error);
    }
  }

  @Get('wagering/transactions/:transactionId')
  public async getTransaction(@Param('transactionId') transactionId: string): Promise<unknown> {
    const transaction = await this.processing.getTransaction(transactionId);
    if (!transaction) {
      throw new NotFoundException({ failureCode: 'TRANSACTION_NOT_FOUND' });
    }
    return transaction;
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  public async getProviderTransaction(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<unknown> {
    const transaction = await this.processing.getProviderTransaction(providerId, externalTransactionId);
    if (!transaction) {
      throw new NotFoundException({ failureCode: 'TRANSACTION_NOT_FOUND' });
    }
    return transaction;
  }

  private rethrow(error: unknown): never {
    if (error instanceof FinancialConflictError) {
      throw new ConflictException({ failureCode: 'IDEMPOTENCY_CONFLICT', message: error.message });
    }
    if (error instanceof FinancialNotFoundError) {
      throw new NotFoundException({ failureCode: 'WALLET_NOT_FOUND', message: error.message });
    }
    if (error instanceof CurrencyMismatchError) {
      throw new BadRequestException({ failureCode: 'CURRENCY_MISMATCH', message: error.message });
    }
    if (error instanceof MoneyValidationError) {
      throw new BadRequestException({ failureCode: 'INVALID_MONEY', message: error.message });
    }
    if (error instanceof DomainRuleError) {
      const status = error.code === 'INSUFFICIENT_BALANCE' ? UnprocessableEntityException : BadRequestException;
      throw new status({ failureCode: error.code, message: error.message });
    }
    throw error;
  }
}
