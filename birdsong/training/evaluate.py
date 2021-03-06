#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 21:10:53 2019

@author: ssharma
"""
#import numpy as np
import torch
from torch.autograd import Variable
#from sklearn import metrics
from .conf_mat import calc_conf_mat


def evaluate(model, data_loader, criterion, num_classes, DEVICE):
    model.eval()
    model = model.to(DEVICE)

    n_correct = 0
    loss = 0

    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(data_loader):
            data, target = Variable(data), Variable(target)
            data = data.float()
            data = data.to(DEVICE)
            target = target.to(DEVICE)

            output = model(data)

            loss += criterion(output, target).item()

            pred = output.data.max(1, keepdim=True)[1]
            n_correct += pred.eq(target.data.view_as(pred)).cpu().sum().item()

            if batch_idx == 0:
                pred_cat = pred
                targ_cat = target
            else:
                pred_cat = torch.cat((pred_cat, pred))
                targ_cat = torch.cat((targ_cat, target))

    conf_matrix = calc_conf_mat(pred_cat, targ_cat, num_classes)

    loss /= len(data_loader)
    acc = n_correct / len(data_loader.dataset)

    return (loss, acc), conf_matrix
