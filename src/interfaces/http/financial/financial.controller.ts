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
  Query,
  Res,
  ServiceUnavailableException,
  UnprocessableEntityException,
} from '@nestjs/common';

import { FinancialConflictError, FinancialNotFoundError, InvalidLedgerCursorError, type OpenWalletCommand, type ProcessBetCommand } from '../../../application/financial-contracts.js';
import { DomainRuleError } from '../../../domain/domain-error.js';
import { CurrencyMismatchError, MoneyValidationError } from '../../../domain/money.js';
import { WagerTransactionStatus } from '../../../domain/wager-transaction.js';
import { isTransientInfrastructureError } from '../../../infrastructure/persistence/transient-errors.js';
import { MikroOrmFinancialProcessing } from '../../../infrastructure/persistence/mikro-orm-financial-processing.js';

/**
 * Minimal structural view of the adapter response object.
 *
 * Declared here instead of importing `express.Response` so the controller does not grow a
 * compile-time dependency on a specific HTTP platform package; only `status` and `json` are
 * used, and both exist on every Nest adapter.
 */
export interface MutableHttpResponse {
  status(code: number): MutableHttpResponse;
  json(body: unknown): unknown;
}

/**
 * Maps a *persisted* transaction status to its HTTP status (CHALLENGE.md section 9).
 *
 * The mapping is driven by durable state, never by the code path that produced it, so an
 * idempotent replay necessarily answers with the same status as the original request.
 */
export function httpStatusForWagerResult(status: WagerTransactionStatus): number {
  switch (status) {
    case WagerTransactionStatus.Processed:
      return 200;
    case WagerTransactionStatus.PendingReference:
      return 202;
    case WagerTransactionStatus.Rejected:
      return 422;
    default:
      return 200;
  }
}

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

  @Get('wallets/:walletId/ledger')
  public async getWalletLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor: string | undefined,
    @Query('limit') limitValue: string | undefined,
  ): Promise<unknown> {
    const limit = limitValue === undefined
      ? 50
      : /^\d+$/.test(limitValue) ? Number.parseInt(limitValue, 10) : Number.NaN;
    try {
      return await this.processing.getWalletLedger(walletId, cursor, limit);
    } catch (error) {
      this.rethrow(error);
    }
  }

  @Post('wallets/:walletId/reconciliation')
  @HttpCode(200)
  public async reconcileWallet(@Param('walletId') walletId: string): Promise<unknown> {
    try {
      return await this.processing.reconcileWallet(walletId);
    } catch (error) {
      this.rethrow(error);
    }
  }

  @Post('wagering/transactions')
  public async processWager(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Res() response: MutableHttpResponse,
  ): Promise<void> {
    const payload = object(body, 'body');
    if (!idempotencyKey) {
      throw new BadRequestException({ failureCode: 'IDEMPOTENCY_KEY_REQUIRED' });
    }
    try {
      const result = await this.processing.processBet({
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
      /**
       * The response is written here instead of being returned, and `@Res()` is used WITHOUT
       * `passthrough`.
       *
       * Reason, verified against the installed @nestjs/core: with `passthrough: true` Nest
       * still treats the response as unhandled and calls
       * `RouterResponseController.apply(result, res, httpStatusCode)`, whose adapter `reply()`
       * executes `response.status(httpStatusCode)` — so the decorator status (`@HttpCode`, or
       * 201 by default for POST) would silently overwrite a status set inside the handler.
       * Writing the response directly is the only variant that actually emits the status
       * derived from durable state at runtime.
       *
       * A durable REJECTED is deliberately NOT thrown as an exception: the unit of work has
       * already committed the rejection and its audit trail, so it is a successful request
       * with a business outcome, not a transport error. The documented body is returned
       * verbatim for 200, 202 and 422 alike — only the status line differs.
       */
      response.status(httpStatusForWagerResult(result.status)).json(result);
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
    if (error instanceof InvalidLedgerCursorError) {
      throw new BadRequestException({ failureCode: 'INVALID_LEDGER_CURSOR', message: error.message });
    }
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
    /**
     * 503 is reserved for an *identified* transient unavailability (see transient-errors.ts:
     * connection-exception / insufficient-resources / operator-intervention SQLSTATEs, plus a
     * concurrency conflict that exhausted its bounded retry budget). In all those cases the
     * transaction was rolled back, so no financial effect exists and resubmitting the same
     * Idempotency-Key is safe — which is exactly what `Retry-After` invites the provider to do.
     *
     * This check is intentionally the LAST one and intentionally narrow: domain errors were
     * already translated above, and any error that is not recognised falls through to the
     * default Nest handling as a 500. Collapsing unknown exceptions into 503 would tell the
     * provider to retry a request that can never succeed and would hide the defect.
     */
    if (isTransientInfrastructureError(error)) {
      throw new ServiceUnavailableException({
        failureCode: 'SERVICE_UNAVAILABLE',
        message: 'Transient infrastructure failure; the request was not applied and may be retried with the same Idempotency-Key',
      });
    }
    throw error;
  }
}
