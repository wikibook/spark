class Record:

    def __init__(self, amount, number=1):
        self.amount = amount
        self.number = number
        
    def addAmt(self, amount):
        return Record(self.amount + amount, self.number + 1)
    
    def __add__(self, other):
        amount = self.amount + other.amount
        number = self.number + other.number 
        return Record(amount, number)
        
    def __str__(self):
        return "avg:" + str(self.amount / self.number)

    def __repr__(self):
        return 'Record(%r, %r)' % (self.amount, self.number)
