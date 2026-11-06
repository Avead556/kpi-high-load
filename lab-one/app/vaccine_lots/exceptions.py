class LotNotFound(Exception):
    def __init__(self, lot_id: int):
        super().__init__(f"vaccine lot {lot_id} not found")
        self.lot_id = lot_id


class LotNumberTaken(Exception):
    def __init__(self, lot_number: str):
        super().__init__(f"lot_number {lot_number} already exists")
        self.lot_number = lot_number
