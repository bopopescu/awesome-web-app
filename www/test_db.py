#!/usr/bin/env
#-*- coding: utf-8 -*-

__author__ = 'Jane'

from models import User,Blog,Comment
from transwarp import db

db.create_engine(user='www-data',password ='www-data',database = 'awesome')
u = User(name = "Test",email = 'test@example.com',password = '12345678',image = 'about:blank')
u.insert()

print 'new user id:', u.id

u1 = User.find_first('where email=?','test@example.com')
print 'find user\s name:',u1.name

u1.delete()

u2 = User.find_first('where email=?','test@example.com')
print 'find user2 :',u2

