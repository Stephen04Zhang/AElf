using System.Linq;
using AElf.CSharp.Core;
using AElf.Sdk.CSharp;
using AElf.Standards.ACS1;
using Google.Protobuf.WellKnownTypes;

namespace AElf.Contracts.MultiToken;

public partial class TokenContract
{
    private Empty CreateNFTCollection(CreateInput input)
    {
        AssertNFTInfoCreateInput(input);
        return CreateToken(input, SymbolType.NFTCollection);
    }

    private Empty CreateNFTInfo(CreateInput input)
    {
        AssertNFTInfoCreateInput(input);
        var nftCollectionInfo = AssertNftCollectionExist(input.Symbol);
        Assert(Context.ChainId == nftCollectionInfo.IssueChainId && Context.ChainId == input.IssueChainId, "NFT create ChainId must be collection's issue chainId");
        Assert(Context.Sender == nftCollectionInfo.Issuer && nftCollectionInfo.Issuer == input.Issuer, "NFT issuer must be collection's issuer");
        return CreateToken(input, SymbolType.NFT);
    }

    private void ChargeCreateFees()
    {
        if (Context.Sender == Context.Origin) return;
        if (IsAddressInCreateWhiteList(Context.Sender)) return;

        var fee = GetCreateMethodFee();
        Assert(fee != null, "not enough balance for create");
        var transferFromInput = new TransferFromInput { From = Context.Sender, To = Context.Self, Symbol = fee.Symbol, Amount = fee.BasicFee };
        TransferFrom(transferFromInput);
        State.Balances[Context.Self][transferFromInput.Symbol] = State.Balances[Context.Self][transferFromInput.Symbol].Sub(transferFromInput.Amount);
        Context.Fire(new TransactionFeeCharged()
        {
            Symbol = transferFromInput.Symbol,
            Amount = transferFromInput.Amount,
            ChargingAddress = Context.Sender
        });
    }

    private MethodFee GetCreateMethodFee()
    {
        var fee = State.TransactionFees[nameof(Create)];
        if (fee == null || fee.Fees.Count <= 0) return new MethodFee { Symbol = Context.Variables.NativeSymbol, BasicFee = 10000_00000000 };
        return fee.Fees.FirstOrDefault(f => GetBalance(Context.Sender, f.Symbol) >= f.BasicFee);
    }

    private string GetNftCollectionSymbol(string inputSymbol)
    {
        var symbol = inputSymbol;
        var words = symbol.Split(TokenContractConstants.NFTSymbolSeparator);
        const int tokenSymbolLength = 1;
        if (words.Length == tokenSymbolLength) return null;
        Assert(words.Length == 2 && words[1].All(IsValidItemIdChar), "Invalid NFT Symbol Input");
        return $"{words[0]}-0";
    }

    private TokenInfo AssertNftCollectionExist(string symbol)
    {
        var collectionSymbol = GetNftCollectionSymbol(symbol);
        if (collectionSymbol == null) return null;
        var collectionInfo = State.TokenInfos[collectionSymbol];
        Assert(collectionInfo != null, "NFT collection not exist");
        return collectionInfo;
    }
}