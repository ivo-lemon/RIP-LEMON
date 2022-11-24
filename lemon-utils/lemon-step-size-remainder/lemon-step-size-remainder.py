import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 

lemon_commissions = {
    'purchase': 0.01,
    'sale': 0.005,
    'swap': 0.003
}
# All this stepsizes are XXX/USDT
# Step sizes @2021-12-07 12:33:00 UTC+1
binance_step_sizes = {
    'BTC': 0.00001,
    'ETH': 0.0001,
    'UNI': 0.01,
    'ADA': 0.1,
    'AXS': 0.01,
    'SLP': 1.0,
    'SOL': 0.01,
    'MANA': 1.0,
    'SAND': 1.0,
    #'MATIC': 0.00001,
    #'ALGO': 0.00001,
    # ['USDTDAI', 0.1], TODO: Ver como agregarlo
}

# userResult(initialAmount) = f(initialAmount - commission*initialAmount)
# lemonResult(initialAmount) = f(initialAmount - commission*initialAmount) - E[step_size] + f(commission*initialAmount)
# Nos interesa: lemonResult(initialAmount) - userResult(initialAmount) =
#   f(initialAmount - commission*initialAmount) + f(commission*initialAmount) - E[step_size] - f(initialAmount - commission*initialAmount) - E[step_size]
#   f(commission*initialAmount) - E[step_size]
# I.e
# initialAmount = 8000
# commission = 80 
# BTC: 0.00001000 => E[0.00001000] = 0.00000500
# User: f(8000 - 80) + 0.00000500
# Lemon (Binance): f(8000 - 80) - 0.00000500 + f(80)
# Lemon - User = f(8000 - 80) - 0.00000500 + f(80) - f(8000 - 80) - 0.0000500
# Commission: known; step_size: known; amount: X
# h1(x) = f(commission * x) - E[step_size]
# Para verlo en pesos hacemos el inverso:
# h2(x) = comission * X - f-1(E[step_size])
# Como E[step_size] = max_step_size / 2
# h2(x) = commission * X - f-1(max_step_size / 2)

# All this stepsizes are XXX/USDT
# Step sizes @2021-12-07 12:33:00 UTC+1
def inverse_rate(x, currency):
    binance_rates = {
        'BTC': 50908.00,
        'ETH': 4363.51,
        'UNI': 17.29,
        'ADA': 1.421,
        'AXS': 107.41,
        'SLP': 0.0389,
        'SOL': 198.55,
        'MANA': 3.8917,
        'SAND': 5.4397,
        'MATIC': 2.272,
        'ALGO': 1.7236,
        # ['USDTDAI', 0.1], TODO: Ver como agregarlo
    }

    arsusd = 220
    return x * binance_rates[currency] * arsusd

# Hacer graficos de h2(x) para todas las currencies 
fig, axs = plt.subplots(3, 4, figsize=(16, 8), sharex=True, sharey=True)#1)
for (comission_index, (comission_concept, comission_value)) in enumerate(lemon_commissions.items()):
    for (step_size_index, (currency, max_step_size)) in enumerate(binance_step_sizes.items()):
        print(comission_index)
        print(step_size_index)
        inv_rate = inverse_rate(max_step_size / 2, currency)
        print(currency)
        print(inv_rate)
        current_ax = axs[comission_index, step_size_index]
        current_ax.plot(
            [x for x in range(0, 50000, 20)], 
            [comission_value * x - inv_rate for x in range(0, 50000, 20)]
        )
        current_ax.plot(
            [x for x in range(0, 50000, 20)], 
            [0 for _ in range(0, 50000, 20)]
        )
        current_ax.set_title(f'{comission_concept} - {currency}')
        current_ax.axvline(x = 10 * 220, linestyle = "--", color = 'k')

plt.setp(axs[-1, :], xlabel="Monto [$]")
plt.setp(axs[:, 0], ylabel='Diferencia')
plt.show()