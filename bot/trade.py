from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum, auto
from typing import Optional, Dict, Any

class TradeStatus(Enum):
    OPEN = auto()
    TRIMMED = auto()
    CLOSED = auto()

class TradeType(Enum):
    CALL = auto()
    PUT = auto()
    STOCK = auto()

@dataclass
class Trade:
    message_id: int
    symbol: str
    status: TradeStatus = TradeStatus.OPEN
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    trade_type: Optional[TradeType] = None
    strike_price: Optional[float] = None
    expiration_date: Optional[str] = None
    creation_time: datetime = field(default_factory=datetime.now)
    last_update_time: datetime = field(default_factory=datetime.now)
    updates: list[str] = field(default_factory=list)

    def __str__(self):
        return f"{self.symbol} {self.trade_type.name if self.trade_type else ''} {self.strike_price if self.strike_price else ''} @ {self.entry_price} | SL: {self.stop_loss} | Status: {self.status.name}"

    def to_dict(self) -> Dict[str, Any]:
        """Converts the Trade object to a dictionary for JSON serialization."""
        trade_dict = asdict(self)
        trade_dict['status'] = self.status.name
        if self.trade_type:
            trade_dict['trade_type'] = self.trade_type.name
        trade_dict['creation_time'] = self.creation_time.isoformat()
        trade_dict['last_update_time'] = self.last_update_time.isoformat()
        return trade_dict

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Trade':
        """Creates a Trade object from a dictionary."""
        data['status'] = TradeStatus[data['status']]
        if data.get('trade_type'):
            data['trade_type'] = TradeType[data['trade_type']]
        data['creation_time'] = datetime.fromisoformat(data['creation_time'])
        data['last_update_time'] = datetime.fromisoformat(data['last_update_time'])
        return cls(**data)
