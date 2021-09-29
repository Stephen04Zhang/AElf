using AElf.CSharp.Core;
using AElf.Sdk.CSharp;
using Google.Protobuf.WellKnownTypes;

namespace AElf.Contracts.NFT
{
    public partial class NFTContract
    {
        private void MakeSureTokenContractAddressSet()
        {
            if (State.TokenContract.Value == null)
            {
                State.TokenContract.Value =
                    Context.GetContractAddressByName(SmartContractConstants.TokenContractSystemName);
            }
        }

        private void MakeSureRandomNumberProviderContractAddressSet()
        {
            if (State.RandomNumberProviderContract.Value == null)
            {
                State.RandomNumberProviderContract.Value =
                    Context.GetContractAddressByName(SmartContractConstants.ConsensusContractSystemName);
            }
        }

        private string GetSymbol(NFTType nftType)
        {
            if (State.NftCount.Value == 0)
            {
                State.NftCount.Value = 10000;
            }

            var number = GenerateSymbolNumber();
            State.IsUsedMap[number] = true;
            return $"{GetPrefix(nftType)}{number}";
        }

        private string GetPrefix(NFTType nftType)
        {
            switch (nftType)
            {
                case NFTType.Any:
                    return "XX";
                case NFTType.Art:
                    return "AR";
                case NFTType.Game:
                    return "GM";
                case NFTType.Cert:
                    return "CE";
                case NFTType.Ticket:
                    return "TI";
            }

            return "XX";
        }

        private long GenerateSymbolNumber()
        {
            var length = GetCurrentNumberLength();
            var from = 1L;
            for (var i = 0; i < length; i++)
            {
                from = from.Mul(10);
            }

            var randomBytes = State.RandomNumberProviderContract.GetRandomBytes.Call(new Int64Value
            {
                Value = Context.CurrentHeight.Sub(1)
            }.ToBytesValue());
            var randomHash =
                HashHelper.ConcatAndCompute(HashHelper.ComputeFrom(Context.Sender),
                    HashHelper.ComputeFrom(randomBytes));
            long randomNumber;
            do
            {
                randomNumber = Context.ConvertHashToInt64(randomHash, from, from.Mul(10));
            } while (State.IsUsedMap[randomNumber]);

            return randomNumber;
        }

        private int GetCurrentNumberLength()
        {
            if (State.SymbolNumberLength.Value == 0)
            {
                State.SymbolNumberLength.Value = NumberMinLength;
            }

            var currentCount = State.NftCount.Value;
            var upper = currentCount.Mul(3).Div(2);
            if (upper.ToString().Length > State.SymbolNumberLength.Value)
            {
                State.SymbolNumberLength.Value = upper.ToString().Length;
                return upper.ToString().Length;
            }

            return State.SymbolNumberLength.Value;
        }
    }
}