from pandas_datareader import data as pdr
import marca as mc

sec=mc.marcap_data('2018-05-04','2021-04-14','086520')
msft=mc.marcap_data('2018-05-04','2021-04-14','237690')

sec_dpc = (sec['Close'] - sec['Close'].shift(1)) / sec['Close'].shift(1) * 100
sec_dpc.iloc[0] = 0
sec_dpc_cs = sec_dpc.cumsum()

msft_dpc = (msft['Close'] - msft['Close'].shift(1)) / msft['Close'].shift(1) * 100
msft_dpc.iloc[0] = 0
msft_dpc_cs = msft_dpc.cumsum()

print(sec_dpc)

# import matplotlib.pyplot as plt
# plt.plot(sec.index, sec_dpc_cs, 'b', label='Samsung Electronics')
# plt.plot(msft.index, msft_dpc_cs, 'r--', label='Microsoft')
# plt.ylabel('Change %')
# plt.grid(True)
# plt.legend('best')
# plt.show()