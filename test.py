for _, row in df.iterrows():
    date_val = row['data']
    ativo_val = row['ativo']
    self.cursor.execute("SELECT COUNT(*) FROM {} WHERE `data` = %s AND `ativo` = %s".format(self.table), (date_val, ativo_val))
    count = self.cursor.fetchone()[0]
    
    if count > 0:
        # Date and 'ativo' combination already exists, perform UPDATE
        update_query = f'''
            UPDATE {self.table}
            SET {', '.join([f"`{col}` = %s" for col in columns])}
            WHERE `data` = %s AND `ativo` = %s
        '''
        update_values = [row[col] for col in columns] + [date_val, ativo_val]
        
        try:
            self.cursor.execute(update_query, update_values)
            self.connection.commit()
        except Exception as e:
            print("Error updating:", str(e))
            self.connection.rollback()  # Rollback changes if an error occurs
        
    # ...
