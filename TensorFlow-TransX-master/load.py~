import tensorflow as tf

sess=tf.Session()    
#First let's load meta graph and restore weights
saver = tf.train.import_meta_graph('model.vec.meta')
saver.restore(sess,tf.train.latest_checkpoint('./'))
#sess.run(tf.global_variables_initializer())

