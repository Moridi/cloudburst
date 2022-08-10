from nvm import pmemobj

# An object to be our root.
class AppRoot(pmemobj.PersistentObject):
    def __init__(self):
        self.accounts = self._p_mm.new(pmemobj.PersistentDict)
        self.A = self._p_mm.new(pmemobj.PersistentList)

    def deposit(self, account, amount):
        self.accounts[account].append(amount)

    def transfer(self, source, sink, amount):
        # Both parts of the transfer will succeed, or neither will.
        with self._p_mm.transaction():
            self.accounts[source].append(-amount)
            self.accounts[sink].append(amount)

    def balance(self, account):
        return sum(self.accounts[account])

    def balances(self):
        for account in self.accounts:
            yield account, self.balance(account)

# Open the object pool, creating it if it doesn't exist yet.
pop = pmemobj.PersistentObjectPool('myaccounts3.pmemobj', flag='c')

# Create an instance of our AppRoot class as the object pool root.
if pop.root is None:
    pop.root = pop.new(AppRoot)

print(pop.root.A)
pop.root.A.append(-1)
print(pop.root.A)
pop.root.A.append(13)
print(pop.root.A)
print(pop.root.A[0])
print(type(pop.root.A[0]))

# # Less typing.
# accounts = pop.root.accounts

# # Make sure two accounts are created.  In a real ap you'd create these
# # accounts with subcommands from the command line.
# for account in ('savings', 'checking'):
#     if account not in accounts:
#         # List of transactions.
#         accounts[account] = pop.new(pmemobj.PersistentList)
#         # Starting balance.
#         accounts[account].append(0)

# # Pretend we have some money.
# pop.root.deposit('savings', 200)

# # Transfer some to checking.
# pop.root.transfer('savings', 'checking', 20)

# # Close and reopen the pool.  The open call will fail if the file
# # doesn't exist.
pop.close()
# pop = pmemobj.PersistentObjectPool('myaccounts.pmemobj')

# # Print the current balances.  In a real ap this would be another
# # subcommand, run at any later time, perhaps after a system reboot...
# for account_name, balance in pop.root.balances():
#     print("{:10s} balance is {:4.2f}".format(account_name, balance))

# # You can run this demo multiple times to see that the deposit and
# # transfer are cumulative.