using System.Linq;
using Google.Protobuf.WellKnownTypes;

namespace AElf.Contracts.MultiToken;

public partial class TokenContract
{
    private Empty CreateNFTCollection(CreateInput input)
    {
        Assert(Context.ChainId == ChainHelper.ConvertBase58ToChainId("AELF"),
            "NFT Protocol can only be created at aelf mainchain.");
        AssertNFTProtocolCreateInput(input);
        return CreateToken(input);
    }

    private Empty CreateNFTInfo(CreateInput input)
    {
        AssertNFTInfoCreateInput(input);
        var nftCollectionInfo = AssertNftCollectionExist(input.Symbol);
        Assert(nftCollectionInfo.Issuer == input.Issuer, "NFT issuer must be collection issuer");
        CreateToken(input);
        return new Empty();
    }

    // private void ChargeNftCreateFees(CreateInput input)
    // {
    //     if (Context.Sender == Context.Origin) return;
    //     var fee = GetMethodFee( new StringValue(){Value = nameof(Create)}).Fees[0];
    //     TransferFrom(new TransferFromInput()
    //     {
    //         From = Context.Sender,
    //         Symbol = fee.Symbol,
    //         Amount = fee.BasicFee,
    //         To =  ;
    //     });
    //     Context.Fire(new );
    // }

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