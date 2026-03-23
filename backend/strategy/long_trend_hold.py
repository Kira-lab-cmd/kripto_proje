from backend.domain.models import SignalData, Sleeve


class LongTrendHoldStrategy:
    name = "trend_hold_long_v1"

    def get_signal(self, symbol: str, price: float, features: dict) -> SignalData:
        atr_pct = features["atr_pct"]
        dir_1h = features["dir_1h"]
        dir_4h = features["dir_4h"]
        ema200_up = features["ema200_up"]
        weekly_structure_ok = features["weekly_structure_ok"]
        regime = features["regime"]

        gate_status = {
            "ema200_up": ema200_up,
            "dir_1h_ok": dir_1h in {"UP", "NEUTRAL"},
            "dir_4h_ok": dir_4h in {"UP", "NEUTRAL"},
            "weekly_structure_ok": weekly_structure_ok,
            "atr_ok": 0.0020 <= atr_pct <= 0.0250,
        }
        score = sum(1 for v in gate_status.values() if v)
        signal = "BUY" if score >= 5 else "HOLD"
        hold_fail = [k for k, v in gate_status.items() if not v]

        return SignalData(
            symbol=symbol,
            sleeve=Sleeve.LONG,
            strategy_name=self.name,
            signal=signal,
            score=float(score),
            buy_threshold=4.0 if signal == "BUY" else 5.0,
            gap_to_threshold=(4.0 if signal == "BUY" else 5.0) - float(score),
            regime=regime,
            dir_1h=dir_1h,
            atr_pct=atr_pct,
            price=price,
            reason=f"long sleeve trend-hold | dir_1h={dir_1h} dir_4h={dir_4h}",
            gate_status=gate_status,
            hold_fail_reasons=hold_fail,
            risk_multiplier=1.0,
        )
