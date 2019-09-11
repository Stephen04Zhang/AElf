using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AElf.Kernel.Blockchain.Application;
using AElf.Kernel.Blockchain.Domain;
using AElf.Kernel.SmartContract;
using AElf.Kernel.SmartContract.Application;
using AElf.Kernel.SmartContract.Domain;
using AElf.Types;
using Microsoft.Extensions.Logging;
using Volo.Abp.DependencyInjection;
using Volo.Abp.EventBus.Local;

namespace AElf.Kernel.SmartContractExecution.Application
{
    public class BlockExecutingService : IBlockExecutingService, ITransientDependency
    {
        private readonly ITransactionExecutingService _executingService;
        private readonly IBlockManager _blockManager;
        private readonly IBlockGenerationService _blockGenerationService;
        public ILocalEventBus EventBus { get; set; }
        public ILogger<BlockExecutingService> Logger { get; set; }

        public BlockExecutingService(ITransactionExecutingService executingService, IBlockManager blockManager,
            IBlockGenerationService blockGenerationService)
        {
            _executingService = executingService;
            _blockManager = blockManager;
            _blockGenerationService = blockGenerationService;
            EventBus = NullLocalEventBus.Instance;
        }

        public async Task<Block> ExecuteBlockAsync(BlockHeader blockHeader,
            IEnumerable<Transaction> nonCancellableTransactions)
        {
            return await ExecuteBlockAsync(blockHeader, nonCancellableTransactions, new List<Transaction>(),
                CancellationToken.None);
        }

        public async Task<Block> ExecuteBlockAsync(BlockHeader blockHeader,
            IEnumerable<Transaction> nonCancellableTransactions, IEnumerable<Transaction> cancellableTransactions,
            CancellationToken cancellationToken)
        {
            Logger.LogTrace("Entered ExecuteBlockAsync");
            var nonCancellable = nonCancellableTransactions.ToList();
            var cancellable = cancellableTransactions.ToList();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            var nonCancellableReturnSets =
                await _executingService.ExecuteAsync(
                    new TransactionExecutingDto {BlockHeader = blockHeader, Transactions = nonCancellable},
                    CancellationToken.None, true);
            var addInfo = $" deal time is {stopWatch.ElapsedMilliseconds}";
            stopWatch.Stop();
            Logger.LogTrace("Executed non-cancellable txs" + addInfo);

            var returnSetCollection = new ReturnSetCollection(nonCancellableReturnSets);
            List<ExecutionReturnSet> cancellableReturnSets = new List<ExecutionReturnSet>();
           stopWatch.Start();
            if (cancellable.Count > 0)
            {
                cancellableReturnSets = await _executingService.ExecuteAsync(new TransactionExecutingDto 
                    {
                        BlockHeader = blockHeader,
                        Transactions = cancellable,
                        PartialBlockStateSet = returnSetCollection.ToBlockStateSet()
                    },
                    cancellationToken, false);
                returnSetCollection.AddRange(cancellableReturnSets);
            }
            stopWatch.Stop();
            var addedInfo = $"###===###handled transaction count is {cancellableReturnSets.Count}" +
                            $"###===###elapsed milliseconds is {stopWatch.ElapsedMilliseconds}";
            Logger.LogTrace("Executed cancellable txs" + addedInfo);

            Logger.LogTrace("Handled return set");

            if (returnSetCollection.Unexecutable.Count > 0)
            {
                await EventBus.PublishAsync(
                    new UnexecutableTransactionsFoundEvent(blockHeader, returnSetCollection.Unexecutable));
            }

            var executed = new HashSet<Hash>(cancellableReturnSets.Select(x => x.TransactionId));
            var allExecutedTransactions =
                nonCancellable.Concat(cancellable.Where(x => executed.Contains(x.GetHash()))).ToList();
            var block = await _blockGenerationService.FillBlockAfterExecutionAsync(blockHeader, allExecutedTransactions,
                returnSetCollection.Executed);

            Logger.LogTrace("Filled block");

            return block;
        }
    }
}