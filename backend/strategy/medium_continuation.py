from backend.domain.models import SignalData, Sleeve


class MediumContinuationStrategy:
    name = "continuation_medium_v1"

    def get_signal(self, symbol: str, price: float, features: dict) -> SignalData:
        atr_pct = features["atr_pct"]
        dir_1h = features["dir_1h"]
        vol_ratio = features["vol_ratio"]
        breakout_ok = features["breakout_ok"]
        retest_ok = features["retest_ok"]
        regime = features["regime"]

        gate_status = {
            "trend_ok": dir_1h in {"UP", "NEUTRAL"},
            "atr_ok": 0.0030 <= atr_pct <= 0.0180,
            "volume_ok": vol_ratio >= 1.10,
            "breakout_ok": breakout_ok,
            "retest_ok": retest_ok,
        }
        score = sum(1 for v in gate_status.values() if v)
        signal = "BUY" if score >= 5 else "HOLD"
        hold_fail = [k for k, v in gate_status.items() if not v]

        return SignalData(
            symbol=symbol,
            sleeve=Sleeve.MEDIUM,
            strategy_name=self.name,
            signal=signal,
            score=float(score),
            buy_threshold=4.0 if signal == "BUY" else 5.0,
            gap_to_threshold=(4.0 if signal == "BUY" else 5.0) - float(score),
            regime=regime,
            dir_1h=dir_1h,
            atr_pct=atr_pct,
            price=price,
            reason=f"medium sleeve continuation | dir_1h={dir_1h} atr_pct={atr_pct:.4f}",
            gate_status=gate_status,
            hold_fail_reasons=hold_fail,
            risk_multiplier=1.0,
        )
