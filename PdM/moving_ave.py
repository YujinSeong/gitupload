import numpy as np


class CurrDetector:
    def __init__(self, ):
        self.max_curr = 0
        self.new_sum_q = 0
        self.moving_ave_curr = 0


    def moving_ave(self, tool_cnt, curr, sum_q):
        curr = np.asarray(curr)
        if sum(curr) == 0:
                raise `ValueError("All Current is zero")`
        if tool_cnt == 0:
            tool_cnt = 1
        if tool_cnt == 1:
            sum_q = 0

        self.max_curr = np.quantile(curr, 0.99)
        self.new_sum_q = sum_q + self.max_curr
        self.moving_ave_curr = self.new_sum_q / tool_cnt
        return self.max_curr, self.new_sum_q, self.moving_ave_curr

    def disp(self):
        print("max_curr: {}, new_sum_q: {}, moving_ave_curr: {}".format(self.max_curr, self.new_sum_q,
                                                                        self.moving_ave_curr))
