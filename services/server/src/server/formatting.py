from lottery import Lottery, Bet

def parse_bet(agency_id: int, bet_line: str) -> Bet:
        fields = bet_line.split(',')
        return Bet(
            agency_id=agency_id,
            first_name=fields[0],
            last_name=fields[1],
            document=int(fields[2]),
            birthdate=fields[3],
            number=int(fields[4])
        )

def format_winner(bet: Bet) -> str:
    return f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"