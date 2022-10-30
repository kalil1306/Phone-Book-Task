import pymongo
import pandas as pd

mongo = pymongo.MongoClient("mongodb+srv://kalil:0204@kalilur2713.poem5n1.mongodb.net/?retryWrites=true&w=majority")

myDB = mongo['D44_08-oct-2022']
collections = myDB['Student_database']
exam_collections = myDB['Exam_database']

def maximum_scores():

        # finding maximum scorer in Exam
        max_score_in_exam = collections.aggregate([
        {'$unwind': '$scores'},
        {'$match': {'scores.type': {'$eq': 'exam'}}}
        ])

        exam_list = []
        for exam in max_score_in_exam:
            get_exam_score = exam['scores']['score']
            exam_list.append(get_exam_score)

        result = max(exam_list)

        for i in collections.find({'scores.0.score': result}):
            print(i['name'], 'is the maximum scorer in exam and the score is', result)

        # finding maximum scorer in quiz
        max_score_in_quiz = collections.aggregate([
            {'$unwind': '$scores'},
            {'$match': {'scores.type': {'$eq': 'quiz'}}}
        ])

        quiz_list = []
        for quiz in max_score_in_quiz:
            get_quiz_score = quiz['scores']['score']
            quiz_list.append(get_quiz_score)

        result = max(quiz_list)

        for i in collections.find({'scores.1.score': result}):
            print(i['name'], 'is the maximum scorer in quiz and the score is', result)

        # finding maximum scorer in homework
        max_score_in_homework = collections.aggregate([
            {'$unwind': '$scores'},
            {'$match': {'scores.type': {'$eq': 'homework'}}}
        ])

        homework_list = []
        for homework in max_score_in_homework:
            get_homework_score = homework['scores']['score']
            homework_list.append(get_homework_score)

        result = max(homework_list)

        for i in collections.find({'scores.2.score': result}):
            print(i['name'], 'is the maximum scorer in quiz and the score is', result)

maximum_scores()































# for a in collections.find():
#     print(a)
# print('-------------------------------------------------------------')

# for i in collections.aggregate([{'$unwind' : { 'path' : '$scores'}}]):
#     print(i)
#
#
#
# print('-------------------------------------------------------------')



# for i in collections.find({}, {'_id' : 1, 'name' : 1, 'scores.0' : 1}):
#     print(i)
#
# for i in collections.aggregate([{'$unwind' :  '$scores'},
#                                 {'$group' : {
#                                     '_id' : '$_id',
#                                     'maximum_score' : {'$max' : '$scores.2.score'}
#                                 }}]):
#     print(i)

# for i in collections.aggregate([{'$unwind' :  '$scores'}]):
#      print(i)




# for a in collections.find({'_id' : 1}):
#     print(a)
#     print('--------------------------------------------')
#
#
# query = collections.aggregate([{'$unwind' :  '$scores'},
#                                {'$project' : {
#                                    'scores' : {
#                                        '$filter' : {
#                                            'input' : '$scores',
#                                            'as' : 'scores',
#                                            'cond' : {'$eq' : ['$scores.type', 'exam']}
#                                        }
#                                    }
#                                }
#                                }
#                                ])
# print(query)




# mylist = pd.DataFrame(collections.aggregate([{'$unwind' :  '$scores'}]))
# print(mylist['scores'])






# pipeline = [{'$unwind' :  '$scores'},
#             {'$match' : {'scores' : {'type' : 'exam'}}}]
#
#
# courses = mongo.myDB.collections.aggregate(pipeline)
#
# print('Kalil is : ', courses)
#
# for a in courses:
#     print(a)


# a = myDB.collections.aggregate({'$match' : {'name': 'Aurelia Menendez'}},
#                           {'$unwind' : '$scores'},
#                           {'$group' : {'_id' : '$scores.type',
# #                                        'average_marks' : {'$avg' : '$scores.score'}}})
# a = collections.aggregate([{
#     '$group' : {
#         '_id' : '$scores'
#     }
# }])
# for i in a:
#     print(i)

# b = collections.aggregate([{'$unwind' : '$scores'},
#                            {'$group' : {'_id' : '$scores'}}])
# for i in b:
#     print(i)


# i = collections.aggregate([{ '$unwind' : '$scores'}])
# for a in i:
#     print(a['scores'])

# for data in collections.find({'scores.type' : 'quiz' }):
#     print(data)

# print((table_view))


#
# type_exam = []
# type_quiz = []
# type_homework = []
# for i in collections.find():
#
#     a_exam = i['scores'][0]['score']
#     b_quiz = i['scores'][1]['score']
#     c_homework = i['scores'][2]['score']
#     type_exam.append(a_exam)
#     type_quiz.append(b_quiz)
#     type_homework.append(c_homework)
#
# print('----------------exam--------------------')
# print(type_exam)
# print('-----------quiz------------')
# print(type_quiz)
# print('--------------homework------------')
# print(type_homework)


#
#
# tb = pd.DataFrame(i)
# print(tb)







    #print(x['scores'][0]['score']) #to retrive the particular doc key in embedded doc
    #unwind operators in OOO used to retrive the embedded document
    #pipelining operators also check- with to retrive the embedded documnet

# i = collections.aggregate([{ '$match' : { 'type' : 'exam'} },
#                            { '$group' : {'_id' : '$name', 'maxscore' : { '$sum' : '$score'}}}
#                            ])
# for j in i:
#     print(j)

