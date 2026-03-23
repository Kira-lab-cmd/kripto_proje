from backend.domain.models import Sleeve


UNIVERSE_TO_SLEEVE = {
    "SUI/USDT": Sleeve.SHORT,
    "NEAR/USDT": Sleeve.SHORT,
    "AVAX/USDT": Sleeve.SHORT,
    "LINK/USDT": Sleeve.MEDIUM,
    "AAVE/USDT": Sleeve.MEDIUM,
    "RNDR/USDT": Sleeve.MEDIUM,
    "BTC/USDT": Sleeve.LONG,
    "ETH/USDT": Sleeve.LONG,
}
