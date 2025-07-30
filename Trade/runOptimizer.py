# call optimizer for params optimization
from Trade.optimizer import optimizer
from Strategy.testStrategy import testStrategy


if __name__ == '__main__':

    PO = optimizer()
    PO.add_parameter('param1', start=0.5, end=1, step=0.5)
    PO.add_parameter('param2', start=0.5, end=1, step=0.5)
    PO.parallel_optimization(testStrategy)
#  TODO: plot
