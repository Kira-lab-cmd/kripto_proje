from backend.domain.models import SignalData, Sleeve


class ShortSnapbackStrategy:
    name = "snapback_short_v1"

    def get_signal(self, symbol: str, price: float, features: dict) -> SignalData:
        atr_pct = features["atr_pct"]
        pullback_score = features["pullback_score"]
        reclaim_score = features["reclaim_score"]
        below_fast = features["below_ema_fast"]
        dir_1h = features["dir_1h"]
        regime = features["regime"]

        score = 0.0
        gate_status = {
            "bias_not_down": dir_1h != "DOWN",
            "atr_ok": 0.0020 <= atr_pct <= 0.0120,
            "pullback_ok": pullback_score >= 0.15,
            "reclaim_ok": reclaim_score >= 0.03,
            "below_ema_fast": below_fast,
        }
        hold_fail = [k for k, v in gate_status.items() if not v]

        if gate_status["bias_not_down"]:
            score += 1
        if gate_status["atr_ok"]:
            score += 1
        if gate_status["pullback_ok"]:
            score += 1
        if gate_status["reclaim_ok"]:
            score += 1
        if gate_status["below_ema_fast"]:
            score += 1

        signal = "BUY" if score >= 5 else "HOLD"
        buy_threshold = 4.0 if signal == "BUY" else 5.0
        gap = buy_threshold - score

        return SignalData(
            symbol=symbol,
            sleeve=Sleeve.SHORT,
            strategy_name=self.name,
            signal=signal,
            score=score,
            buy_threshold=buy_threshold,
            gap_to_threshold=gap,
            regime=regime,
            dir_1h=dir_1h,
            atr_pct=atr_pct,
            price=price,
            reason=f"short sleeve snapback | dir_1h={dir_1h} atr_pct={atr_pct:.4f}",
            gate_status=gate_status,
            hold_fail_reasons=hold_fail,
            risk_multiplier=1.0,
        )
