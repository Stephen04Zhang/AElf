using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AElf.CSharp.Core;
using AElf.Types;
using Google.Protobuf.WellKnownTypes;
using Shouldly;
using Xunit;

namespace AElf.Contracts.MultiToken;

public static class NftCollectionMetaFields
{
    public static string ImageUrlKey = "__nft_image_url";
    public static string BaseUriKey = "__nft_base_uri";
    public static string NftType = "__nft_type";
    public static string MinterListKey = "__nft_minter_list";
    public const string IsItemIdReuseKey = "__nft_is_item_id_reuse";
}

public static class NftInfoMetaFields
{
    public static string ImageUrlKey = "__nft_image_url";
    public static string IsBurnedKey = "__nft_is_burned";
}

public partial class MultiTokenContractTests
{
    private readonly string NftType = "ART";

    private TokenInfo NftCollection721Info => new()
    {
        Symbol = "TT-",
        TokenName = "Trump Digital Trading Cards #721",
        TotalSupply = 1,
        Decimals = 0,
        Issuer = Accounts[0].Address,
        IssueChainId = _chainId,
        ExternalInfo = new ExternalInfo()
        {
            Value = { { NftCollectionMetaFields.ImageUrlKey, "https://i.seadn.io/gcs/files/0f5cdfaaf687de2ebb5834b129a5bef3.png?auto=format&w=3840" } }
        }
    };

    private TokenInfo NftCollection1155Info => new()
    {
        Symbol = "TP-",
        TokenName = "Trump Digital Trading Cards #1155",
        TotalSupply = TotalSupply,
        Decimals = 0,
        Issuer = Accounts[0].Address,
        IssueChainId = _chainId,
        ExternalInfo = new ExternalInfo()
        {
            Value = { { NftCollectionMetaFields.ImageUrlKey, "https://i.seadn.io/gcs/files/0f5cdfaaf687de2ebb5834b129a5bef3.png?auto=format&w=3840" }, { NftCollectionMetaFields.NftType, NftType } }
        }
    };

    private TokenInfo NftInfo => new()
    {
        Symbol = "31175",
        TokenName = "Trump Digital Trading Card #31175",
        TotalSupply = TotalSupply,
        Decimals = 0,
        Issuer = Accounts[0].Address,
        IssueChainId = _chainId,
        IsBurnable = true,
        ExternalInfo = new ExternalInfo()
        {
            Value = { { NftInfoMetaFields.ImageUrlKey, "https://i.seadn.io/gcs/files/0f5cdfaaf687de2ebb5834b129a5bef3.png?auto=format&w=3840" } }
        }
    };

    private TokenInfo NftInfo2 => new()
    {
        Symbol = "12419",
        TokenName = "Trump Digital Trading Card #12419",
        TotalSupply = TotalSupply,
        Decimals = 0,
        Issuer = Accounts[0].Address,
        IssueChainId = _chainId,
        IsBurnable = false,
        ExternalInfo = new ExternalInfo()
        {
            Value = { { NftInfoMetaFields.ImageUrlKey, "https://i.seadn.io/gcs/files/0f5cdfaaf687de2ebb5834b129a5bef3.png?auto=format&w=3840" }, { NftInfoMetaFields.IsBurnedKey, "false" } }
        }
    };

    private async Task<IExecutionResult<Empty>> CreateNftCollectionAsync(TokenInfo collectionInfo)
    {
        await CreateTokenAndIssue();
        return await TokenContractStub.Create.SendAsync(new CreateInput
        {
            Symbol = $"{collectionInfo.Symbol}0",
            TokenName = collectionInfo.TokenName,
            TotalSupply = collectionInfo.TotalSupply,
            Decimals = collectionInfo.Decimals,
            Issuer = collectionInfo.Issuer,
            IssueChainId = collectionInfo.IssueChainId,
            ExternalInfo = collectionInfo.ExternalInfo
        });
    }

    private async Task<IExecutionResult<Empty>> CreateNftAsync(string colllectionSymbol, TokenInfo nftInfo)
    {
        return await TokenContractStub.Create.SendAsync(new CreateInput
        {
            Symbol = $"{colllectionSymbol}{nftInfo.Symbol}",
            TokenName = nftInfo.TokenName,
            TotalSupply = nftInfo.TotalSupply,
            Decimals = nftInfo.Decimals,
            Issuer = nftInfo.Issuer,
            IsBurnable = nftInfo.IsBurnable,
            IssueChainId = nftInfo.IssueChainId,
            ExternalInfo = nftInfo.ExternalInfo
        });
    }

    private async Task<List<string>> CreateNftCollectionAndNft(bool reuseItemId = true)
    {
        var symbols = new List<string>();
        var collectionInfo = reuseItemId ? NftCollection1155Info : NftCollection721Info;
        var createCollectionRes = await CreateNftCollectionAsync(collectionInfo);
        createCollectionRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);
        var createCollectionLog = TokenCreated.Parser.ParseFrom(createCollectionRes.TransactionResult.Logs.First(l => l.Name == nameof(TokenCreated)).NonIndexed);
        var collectionSymbolWords = createCollectionLog.Symbol.Split("-");
        Assert.True(collectionSymbolWords.Length == 2);
        AssertTokenEqual(createCollectionLog, collectionInfo);
        symbols.Add(createCollectionLog.Symbol);
        createCollectionLog.Symbol.ShouldBe(collectionInfo.Symbol + "0");

        var createNftRes = await CreateNftAsync(collectionInfo.Symbol, NftInfo);
        createNftRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);
        var createNftLog = TokenCreated.Parser.ParseFrom(createNftRes.TransactionResult.Logs.First(l => l.Name == nameof(TokenCreated)).NonIndexed);
        var nftSymbolWords = createNftLog.Symbol.Split("-");
        Assert.True(nftSymbolWords.Length == 2);
        Assert.Equal(nftSymbolWords[0], collectionSymbolWords[0]);
        AssertTokenEqual(createNftLog, NftInfo);
        symbols.Add(createNftLog.Symbol);
        createNftLog.Symbol.ShouldBe(collectionInfo.Symbol + NftInfo.Symbol);

        var createNft2Res = await CreateNftAsync(collectionInfo.Symbol, NftInfo2);
        createNft2Res.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);
        var createNft2Log = TokenCreated.Parser.ParseFrom(createNft2Res.TransactionResult.Logs.First(l => l.Name == nameof(TokenCreated)).NonIndexed);
        var nft2SymbolWords = createNft2Log.Symbol.Split("-");
        Assert.True(nft2SymbolWords.Length == 2);
        Assert.Equal(nft2SymbolWords[0], collectionSymbolWords[0]);
        AssertTokenEqual(createNft2Log, NftInfo2);
        symbols.Add(createNft2Log.Symbol);
        createNft2Log.Symbol.ShouldBe(collectionInfo.Symbol + NftInfo2.Symbol);
        return symbols;
    }

    private void AssertTokenEqual(TokenCreated log, TokenInfo input)
    {
        Assert.Equal(log.TokenName, input.TokenName);
        Assert.Equal(log.TotalSupply, input.TotalSupply);
        Assert.Equal(log.Decimals, input.Decimals);
        Assert.Equal(log.Issuer, input.Issuer);
        Assert.Equal(log.IssueChainId, input.IssueChainId);
        foreach (var kv in input.ExternalInfo.Value)
        {
            Assert.True(log.ExternalInfo.Value.ContainsKey(kv.Key));
            Assert.Equal(log.ExternalInfo.Value[kv.Key], kv.Value);
        }
    }

    [Fact(DisplayName = "[MultiToken_Nft] Create 1155 nfts.")]
    private async Task MultiTokenContract_Create_1155Nft_Test()
    {
        await CreateNftCollectionAndNft();
    }

    [Fact(DisplayName = "[MultiToken_Nft] Create 721 nfts.")]
    private async Task MultiTokenContract_Create_721Nft_Test()
    {
        await CreateNftCollectionAndNft(false);
    }


    [Fact(DisplayName = "[MultiToken_Nft] issue and transfer 1155 nfts.")]
    private async Task NftIssueAndTransferTest()
    {
        var symbols = await CreateNftCollectionAndNft();
        Assert.True(symbols.Count == 3);
        var issueRes = await TokenContractStub.Issue.SendAsync(new IssueInput()
        {
            Symbol = symbols[1],
            Amount = 100,
            To = DefaultAddress,
            Memo = "Issue Nft"
        });
        issueRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var nftInfo = await TokenContractStub.GetTokenInfo.CallAsync(new GetTokenInfoInput() { Symbol = symbols[1] });
        nftInfo.Supply.ShouldBe(100);
        nftInfo.Issuer.ShouldBe(NftCollection721Info.Issuer);

        var transferRes = await TokenContractStub.Transfer.SendAsync(new TransferInput()
        {
            Amount = 10,
            Memo = "transfer nft test",
            Symbol = symbols[1],
            To = User1Address
        });
        transferRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var defaultBalanceOutput = await TokenContractStub.GetBalance.CallAsync(new GetBalanceInput
        {
            Symbol = symbols[1],
            Owner = DefaultAddress
        });
        var user1BalanceOutput = await TokenContractStub.GetBalance.CallAsync(new GetBalanceInput
        {
            Symbol = symbols[1],
            Owner = User1Address
        });

        defaultBalanceOutput.Balance.ShouldBe(90);
        user1BalanceOutput.Balance.ShouldBe(10);
    }

    [Fact(DisplayName = "[MultiToken_Nft] issue and transfer 721 nfts.")]
    private async Task Nft721IssueAndTransferTest()
    {
        var symbols = await CreateNftCollectionAndNft(false);
        Assert.True(symbols.Count == 3);
        var issueRes = await TokenContractStub.Issue.SendAsync(new IssueInput()
        {
            Symbol = symbols[1],
            Amount = 1,
            To = DefaultAddress,
            Memo = "Issue Nft"
        });
        issueRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var nftInfo = await TokenContractStub.GetTokenInfo.CallAsync(new GetTokenInfoInput() { Symbol = symbols[1] });
        nftInfo.Supply.ShouldBe(1);
        nftInfo.Issuer.ShouldBe(NftCollection721Info.Issuer);

        var transferRes = await TokenContractStub.Transfer.SendWithExceptionAsync(new TransferInput()
        {
            Amount = 10,
            Memo = "transfer nft test",
            Symbol = symbols[1],
            To = User1Address
        });
        transferRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Failed);

        transferRes = await TokenContractStub.Transfer.SendAsync(new TransferInput()
        {
            Amount = 1,
            Memo = "transfer nft test",
            Symbol = symbols[1],
            To = User1Address
        });
        transferRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var defaultBalanceOutput = await TokenContractStub.GetBalance.CallAsync(new GetBalanceInput
        {
            Symbol = symbols[1],
            Owner = DefaultAddress
        });
        var user1BalanceOutput = await TokenContractStub.GetBalance.CallAsync(new GetBalanceInput
        {
            Symbol = symbols[1],
            Owner = User1Address
        });

        defaultBalanceOutput.Balance.ShouldBe(0);
        user1BalanceOutput.Balance.ShouldBe(1);
    }

    [Fact]
    private async Task NftIssueAndTransfer_OutOfAmount_Test()
    {
        var symbols = await CreateNftCollectionAndNft();
        Assert.True(symbols.Count == 3);

        var transferRes = await TokenContractStub.Transfer.SendWithExceptionAsync(new TransferInput()
        {
            Amount = 10,
            Memo = "transfer nft test",
            Symbol = symbols[1],
            To = DefaultAddress
        });
        transferRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Failed);
        transferRes.TransactionResult.Error.ShouldContain("Can't do transfer to sender itself");

        transferRes = await TokenContractStub.Transfer.SendWithExceptionAsync(new TransferInput()
        {
            Amount = 10,
            Memo = "transfer nft test",
            Symbol = symbols[1],
            To = User1Address
        });
        transferRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Failed);
        transferRes.TransactionResult.Error.ShouldContain("Insufficient balance");
    }

    [Fact(DisplayName = "[MultiToken] Token Burn Test")]
    private async Task NftIssueAndTransferBurn()
    {
        var symbols = await CreateNftCollectionAndNft();
        Assert.True(symbols.Count == 3);
        var issueRes = await TokenContractStub.Issue.SendAsync(new IssueInput()
        {
            Symbol = symbols[1],
            Amount = 100,
            To = DefaultAddress,
            Memo = "Issue Nft"
        });
        issueRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var res = await TokenContractStub.Burn.SendAsync(new BurnInput
        {
            Amount = 10,
            Symbol = symbols[1]
        });
        res.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var balance = await TokenContractStub.GetBalance.CallAsync(new GetBalanceInput
        {
            Owner = DefaultAddress,
            Symbol = symbols[1]
        });
        balance.Balance.ShouldBe(90);

        var result = (await TokenContractStub.Burn.SendWithExceptionAsync(new BurnInput
        {
            Symbol = symbols[1],
            Amount = 3000L
        })).TransactionResult;
        result.Status.ShouldBe(TransactionResultStatus.Failed);
        result.Error.ShouldContain("Insufficient balance");
    }

    [Fact(DisplayName = "[MultiToken] Token Burn Test")]
    private async Task NftIssueAndTransfer_Approve()
    {
        var symbols = await CreateNftCollectionAndNft();
        Assert.True(symbols.Count == 3);
        var issueRes = await TokenContractStub.Issue.SendAsync(new IssueInput()
        {
            Symbol = symbols[1],
            Amount = 100,
            To = DefaultAddress,
            Memo = "Issue Nft"
        });
        issueRes.TransactionResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var approveBasisResult = (await TokenContractStub.Approve.SendAsync(new ApproveInput
        {
            Symbol = symbols[1],
            Amount = 10,
            Spender = BasicFunctionContractAddress
        })).TransactionResult;
        approveBasisResult.Status.ShouldBe(TransactionResultStatus.Mined);

        var basicBalanceOutput = await TokenContractStub.GetBalance.CallAsync(new GetBalanceInput
        {
            Owner = BasicFunctionContractAddress,
            Symbol = symbols[1]
        });
        basicBalanceOutput.Balance.ShouldBe(0);

        var basicAllowanceOutput = await TokenContractStub.GetAllowance.CallAsync(new GetAllowanceInput
        {
            Owner = DefaultAddress,
            Spender = BasicFunctionContractAddress,
            Symbol = symbols[1]
        });
        basicAllowanceOutput.Allowance.ShouldBe(10);
    }
}