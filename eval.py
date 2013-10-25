#!/usr/bin/env python


import numpy as np
import scipy.sparse as sp
import sys


def loadsparse(fname):
    """
    Load sparse matrix from CSV file

    :rtype: :class:`~scipy.sparse.coo_matrix`"""
    print "Loading %s ..." % (fname)

    row, col, data = [], [], []
    with open(fname) as f:
        # f.readline()

        for line in f:
            tok = line.split("|")
            row.append(int(tok[0]))
            col.append(int(tok[1]))
            data.append(float(tok[2]))
    return sp.coo_matrix((data, (row, col)), (np.max(row) + 1, np.max(col) + 1))

def loaddense(fname):
    """
    Load dense matrix from CSV file, with index as first column
    """
    print "Loading %s ..." % (fname)
    #return np.asmatrix(np.loadtxt(open(fname,"rb"),delimiter=" ",skiprows=0))
    nda = np.loadtxt(open(fname,"rb"),delimiter=" ",skiprows=0)
    ids = nda[:,0].astype(int)
    data = nda[:,1:]
    return data[ids]
    
def objfunc(R, P, Q, lambd):
    sse = (((P[R.row] * Q[R.col]).sum(1) - R.data)**2).sum()
    return sse + lambd * (P * P).sum() + lambd * (Q * Q).sum()

def eval(R, P, Q, lambd = 1e-6):
    print "R: %s" % (str(R.shape))
    print "P: %s" % (str(P.shape))
    print "Q: %s" % (str(Q.shape))
    obj = objfunc(R, P, Q, lambd)
    print "obj: %.2f" % (obj)

if __name__ == '__main__':
    eval(loadsparse(sys.argv[1]), loaddense(sys.argv[2]), loaddense(sys.argv[3]), lambd=0)