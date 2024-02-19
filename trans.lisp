(ql:quickload "cl-ppcre")
;(load "entity_db.lisp")

(defparameter *table-names*
  '((見積ヘッダ MITSUMORIHEDDA "mh")
    (発注ヘッダ HACCHUUHEDDA "hh")
    (検収ヘッダ KENSHUUHEDDA "kh")
    (購買業務 KOUBAIGYOUMU "kbg")
    ))

(defparameter *column-names*
  '(("見積ヘッダ" "物件名" BUKKENMEI)
    ("発注ヘッダ" "物件名" BUKKENMEI)
    ("発注ヘッダ" "物件種類" BUKKENSHURUI)
    ("検収ヘッダ" "物件名" BUKKENMEI)
    ("検収ヘッダ" "検収予定日" KENSHUUYOTEIBI)
    ("購買業務" "購買業務管理番号" KOUBAIGYOUMUKANRINO)
    ))


(defparameter *input-sqlfile* "test.sql")

(defvar *input* nil)
(defvar *select-area* nil)
(defvar *from-area* nil)
(defvar *join-area* nil)
(defvar *where-area* nil)
(defvar *order-group-area* nil)

(defun main ()
  (init)
  (setf *input* (read-sql-file))
  (set-areas (split-area *input*))
  (print-sql)
  (write-sql))

(defun init ()
  (setf *input* nil)
  (setf *select-area* nil)
  (setf *from-area* nil)
  (setf *join-area* nil)
  (setf *where-area* nil)
  (setf *order-group-area* nil))

(defun read-sql-file ()
  (let ((ret nil))
    (with-open-file (input *input-sqlfile* :direction :input)
      (loop
	(let ((buf (read-line input nil)))
	  (when (null buf) (return ret))
	  (setf ret (cons buf ret)))))))

(defun set-areas (splitted)
  (setf *select-area* (first splitted))
  (setf *from-area* (second splitted))
  (setf *join-area* (third splitted))
  (setf *where-area* (fourth splitted))
  (setf *order-group-area* (fifth splitted)))

(defun split-area (input)
  (let ((sa nil) (fa nil) (ja nil) (wa nil) (oa nil))
    (dolist (line (reverse input))
      (let ((clause (find-clause line)))
	(cond ((equal "SELECT" clause)
	       (print "find SELECT")
	       (setf mode 1))
	      ((equal "FROM" clause)
	       (print "find FROM")
	       (setf fa (cons (generate-from-area line) fa))
	       (setf mode 2))
	      ((equal "JOIN" clause)
	       (print "find JOIN")
	       (setf ja (cons (generate-join-line line) ja))
	       (setf mode 3))
	      ((equal "WHERE" clause)
	       (print "find WHERE")
	       (setf wa (cons "todo" wa))
	       (setf mode 4))
	      ((or (equal "ORDER" clause)
		   (equal "GROUP" clause))
	       (push (format nil "~&~A BY " clause) oa)
	       (setf mode 5))
	      (t (case mode
		   (1 (progn ;the first column of SELECT
			(setf sa (cons (generate-select-area line mode) sa))
			(setf mode 1.1)))
		   (1.1 (setf sa (cons (generate-select-area line mode) sa)))
		   (3 (setf ja (cons (generate-join-area line) ja)))
		   (5 (setf oa (cons (generate-order-group-area line) oa)))
		   )))))
    (list sa fa ja wa oa)))

(defun find-clause (line)
  (ppcre:register-groups-bind
      (clause)
      ("(SELECT|FROM|JOIN|WHERE|ORDER|GROUP)\\s*" (string-left-trim " " line))
    clause))
  
(defun generate-select-area (line mode)
  (let ((tname (parse-get-table-name select line))
	(cname (parse-get-table-short-name select line)))
    (case mode
      (1 (format nil "~&~A.~A " tname cname))
      (1.1 (format nil "~&,~A.~A " tname cname)))))
  
(defun parse-select-area (line)
  (ppcre:register-groups-bind
      (table column)
      ("[,\\s*]*(\\S+)\\.(\\S+)" (string-trim " " line))
    (list table column)))

(defun generate-from-area (line)
  (let ((table (parse-from-area line)))
    (format nil "~&FROM ~A ~A "
	    (get-table-name table *table-names*)
	    (get-table-short-name table *table-names*))))

(defun parse-from-area (line)
  (ppcre:register-groups-bind
      (table)
      ("FROM\\s+(.+)" (string-trim " " line))
    table))
      
(defun generate-join-line (line)
  (let ((table (parse-join-line line)))
    (ppcre:regex-replace "JOIN\\s+(\\S+)\\s+ON"
			 line
			 (format nil
				 "JOIN ~A ~A ON "
				 (get-table-name table *table-names*)
				 (get-table-short-name table *table-names*)))))
				      		 
(defun parse-join-line (line)
  (ppcre:register-groups-bind
      (table)
      ("JOIN\\s+(\\S+)\\s+ON" line)
    table))

(defun generate-join-area (line)
  (let* ((cnt (ppcre:count-matches "AND" line))
	 (items (parse-join-area line))
	 (tc (get-tshort-column-name (first items) (second items)
				     *table-names* *column-names*))
	 (tc2 (get-tshort-column-name (third items) (fourth items)
				      *table-names* *column-names*))
	 (tname (first tc))
	 (cname (second tc))
	 (tname2 (first tc2))
	 (cname2 (second tc2)))
    (cond ((= 0 cnt)
	   (format nil "~&~A.~A = ~A.~A " tname cname tname2 cname2))
	  (t (format nil "~&AND ~A.~A = ~A.~A " tname cname tname2 cname2)))))

(defun parse-join-area (line)
  (ppcre:register-groups-bind
      (table column table2 column2)
      ("[,\\s*]*(\\S+)\\.(\\S+) = (\\S+)\\.(\\S+)" (string-trim " " line))
    (list table column table2 column2)))

;todo
(defun generate-where-line (line)
  (let ((table (parse-where-line line)))
    (ppcre:regex-replace "WHERE\\s+(\\S+).(\\S+) = (\\S+).(\\S+) "
			 line
			 (format nil
				 "WHERE ~A.~A = ~A.~A "
				 (get-table-name table *table-names*)
				 (get-table-short-name table *table-names*)
				 "todo"
				 "todo"))))
				      		 
(defun parse-where-line (line)
  (ppcre:register-groups-bind
      (t1 c1 t2 c2)
      ("WHERE\\s+(\\S+).(\\S+) = (\\S+).(\\S+)" (string-trim " "line))
    (list t1 c1 t2 c2)))


(defun generate-order-group-area (line)
  (let* ((tmp (parse-order-group-area line))
	 (tname (get-table-short-name (first tmp) *table-names*))
	 (cname (get-column-name (first tmp) (second tmp) *column-names*)))	
    (format nil "~A.~A " tname cname)))

(defun parse-order-group-area (line)
  (ppcre:register-groups-bind
      (table column)
      ("(\\S+)\\.(\\S+)" (string-trim " " line))
    (list table column)))

(defun get-table-name (table db)
  (second (assoc table db :test #'string-equal)))

(defun get-table-short-name (table db)
  (third (assoc table db :test #'string-equal)))

(defun get-column-name (table column db)
;  (find '物件種類 (remove-if-not (lambda (x) (eq (car x) '発注ヘッダ)) *column-names*) :key #'cadr)
  (third (find column (remove-if-not #'(lambda (x) (equal (car x) table))
				     *column-names*)
	       :key #'cadr :test #'string-equal)))

;購買業務.物件名 のようなペアを返す
(defun get-tshort-column-name (table column tdb cdb)
  (let* ((tname (get-table-short-name table tdb))
	 (cname (get-column-name table column cdb)))
    (list tname cname)))

;mode: select, order, group
;ex:購買業務.物件名 のような形が1行に1回だけのエリア
(defmacro parse-get-table-name (mode line)
  (let ((fname (intern (format nil "PARSE-~A-AREA" mode))))
    `(let ((table (first (,fname ,line))))
       (get-table-name table *table-names*))))
       
(defmacro parse-get-table-short-name (mode line)
  (let ((fname (intern (format nil "PARSE-~A-AREA" mode))))
    `(let ((table (first (,fname ,line))))
       (get-table-short-name table *table-names*))))



(defun print-sql ()
  (print "SELECT")
  (dolist (line (reverse *select-area*))
    (print line))
  (print (first *from-area*))
  (dolist (line (reverse *join-area*))
    (print line))
  (print *where-area*)
  (dolist (line (reverse *order-group-area*))    
    (print line)))

(defun write-sql ()
  (with-open-file (output "result.file"
			  :direction :output
			  :if-exists :supersede)    
    (write-line "SELECT" output)
    (dolist (line (reverse *select-area*))
      (write-line line output))
    (write-line (first *from-area*) output)
    (dolist (line (reverse *join-area*))
      (write-line line output))
    (write-line (first *where-area*) output)
    (dolist (line (reverse *order-group-area*))
      (write-line line output))))

