from lottery import Lottery, Bet

def parse_bet(agency_id: int, bet_line: str) -> Bet:
    """
    Parses a bet line into a Bet object.
    Args:
        agency_id (int): The ID of the agency placing the bet.
        bet_line (str): A string representing the bet, formatted as `first_name,last_name,document,birthdate,number`.
    Returns:
        Bet: An instance of the Bet class containing the parsed information.
    """
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
    """
    Formats a Bet object into a string representation for winners.
    Args:
        bet (Bet): An instance of the Bet class representing a winning bet.
    Returns:
        str: A string formatted as `first_name,last_name,document,birthdate,number`.
    """
    return f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"